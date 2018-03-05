package cmd

import (
	"bytes"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/pipeline-subscribers/shapeutils"
)

const MySQLTimeFormat = "2006-01-02 15:04:05"

const createTemplateText = `CREATE OR REPLACE TABLE {{tick .Name}} ({{range .Columns}}
	{{tick .Name}} {{.SqlType}} {{if .IsKey}}NOT {{end}}NULL,{{end}}{{range .VirtualColumns}}
	{{tick .Name}} {{.SqlType}} AS ({{tick .FromName}}) VIRTUAL,{{end}}
	{{tick "naveegoPublisher"}} VARCHAR(1000) DEFAULT NULL,
	{{tick "naveegoPublishedAt"}} DATETIME DEFAULT NULL,
	{{tick "naveegoCreatedAt"}} DATETIME DEFAULT CURRENT_TIMESTAMP,
	{{tick "naveegoShapeVersion"}} VARCHAR(50) DEFAULT NULL,
	{{if gt (len .Keys) 0}}PRIMARY KEY ({{jointick .Keys}}){{end}}
);
`

const createViewTemplateText = `CREATE OR REPLACE VIEW {{tick .VirtualName}} ({{range .VirtualColumns}}
	{{tick .Name}},{{end}}
	{{tick "naveegoPublisher"}},
	{{tick "naveegoPublishedAt"}},
	{{tick "naveegoCreatedAt"}},
	{{tick "naveegoShapeVersion"}}
)
AS SELECT {{range .VirtualColumns}}
	{{tick .FromName}},{{end}}
	{{tick "naveegoPublisher"}},
	{{tick "naveegoPublishedAt"}},
	{{tick "naveegoCreatedAt"}},
	{{tick "naveegoShapeVersion"}}
FROM {{tick .Name}};
`

const alterTemplateText = `ALTER TABLE {{tick .Name}}{{range $i, $e := .Columns}}
	{{if $i}},{{end}}ADD COLUMN IF NOT EXISTS {{tick $e.Name}} {{$e.SqlType}} {{if $e.IsKey}}NOT {{end}}NULL{{end}}{{if gt (len .Keys) 0}}
	,DROP PRIMARY KEY
	,ADD PRIMARY KEY ({{jointick .Keys}}){{end}};`

const upsertTemplateText = `INSERT INTO {{tick .Name}} ({{range $i, $e := .Columns}}{{tick $e.Name}}, {{end}}
	{{tick "naveegoPublisher"}}, {{tick "naveegoPublishedAt"}}, {{tick "naveegoShapeVersion"}})
	VALUES ({{range $i, $e := .Columns}}?, {{end}}?, ?, ?)
	ON DUPLICATE KEY UPDATE{{range $i, $e := .NonKeyColumns}}{{if not $e.IsKey}}
		{{tick $e.Name}} = VALUES({{tick $e.Name}}),{{end}}{{end}}
		{{tick "naveegoPublisher"}} = VALUES({{tick "naveegoPublisher"}}),
		{{tick "naveegoPublishedAt"}} = VALUES({{tick "naveegoPublishedAt"}}),
		{{tick "naveegoShapeVersion"}} = VALUES({{tick "naveegoShapeVersion"}});`

var (
	alterTemplate      *template.Template
	createTemplate     *template.Template
	createViewTemplate *template.Template
	upsertTemplate     *template.Template
)

func init() {
	funcs := template.FuncMap{
		"tick":     func(item string) string { return "`" + item + "`" },
		"join":     func(items []string) string { return strings.Join(items, "`, `") },
		"jointick": func(items []string) string { return "`" + strings.Join(items, "`, `") + "`" },
	}
	alterTemplate = template.Must(template.New("alter").
		Funcs(funcs).
		Parse(alterTemplateText))

	createTemplate = template.Must(template.New("create").
		Funcs(funcs).
		Parse(createTemplateText))

	createViewTemplate = template.Must(template.New("create").
		Funcs(funcs).
		Parse(createViewTemplateText))

	upsertTemplate = template.Must(template.New("upsert").
		Funcs(funcs).
		Parse(upsertTemplateText))

}

func createShapeChangeSQL(shapeInfo shapeutils.ShapeDelta, viewName string) (string, error) {

	var (
		err error
		w   = &bytes.Buffer{}
	)

	model := sqlTableModel{
		Name:        escapeString(shapeInfo.Name),
		Keys:        shapeInfo.NewKeys,
		VirtualName: viewName,
	}

	if !shapeInfo.IsNew {
		// there's a previous shape to consider,
		// we need to include its keys when re-creating the PK
		for _, k := range shapeInfo.PreviousShape.Keys {
			model.Keys = append(model.Keys, k)
		}
	}

	for n, t := range shapeInfo.NewProperties {
		columnModel := sqlColumnModel{
			Name: escapeString(n),
		}
		for _, k := range model.Keys {
			if k == n {
				columnModel.IsKey = true
			}
		}
		columnModel.SqlType = convertToSQLType(t, columnModel.IsKey)

		model.Columns = append(model.Columns, columnModel)

		if friendlyName, ok := shapeInfo.NewPropertyNameToVirtualName[n]; ok {
			vm := virtualSqlColumnModel{
				sqlColumnModel: sqlColumnModel{
					Name:    escapeString(friendlyName),
					SqlType: columnModel.SqlType,
					IsKey:   columnModel.IsKey,
				},
				FromName: columnModel.Name,
			}
			model.VirtualColumns = append(model.VirtualColumns, vm)
		}

	}

	sort.Sort(model.Columns)
	for _, c := range model.Columns {
		if !c.IsKey {
			model.NonKeyColumns = append(model.NonKeyColumns, c)
		}
	}

	if shapeInfo.IsNew {
		err = createTemplate.Execute(w, model)
		if err == nil {
			err = createViewTemplate.Execute(w, model)
		}
	} else {
		if !shapeInfo.HasKeyChanges {
			model.Keys = nil
		}
		err = alterTemplate.Execute(w, model)
	}

	command := w.String()

	return command, err
}

type sqlTableModel struct {
	Name           string
	VirtualName    string
	Columns        sqlColumns
	VirtualColumns []virtualSqlColumnModel
	NonKeyColumns  sqlColumns
	Keys           []string
}

type sqlColumns []sqlColumnModel

type sqlColumnModel struct {
	Name    string
	SqlType string
	IsKey   bool
}

type virtualSqlColumnModel struct {
	sqlColumnModel
	FromName string
}

func (s sqlColumns) Len() int {
	return len(s)
}
func (s sqlColumns) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sqlColumns) Less(i, j int) bool {
	return strings.Compare(s[i].Name, s[j].Name) < 0
}

const (
	keyUpsertSQL        = "UpsertSQL"
	keyParameterOrderer = "ParameterOrder"
)

func createUpsertSQL(datapoint pipeline.DataPoint, knownShape *shapeutils.KnownShape) (sql string, params []interface{}, err error) {

	var (
		// 	gotOrderer bool
		orderer func(pipeline.DataPoint) []interface{}
	)

	// item, gotSQL := knownShape.Get(keyUpsertSQL)
	// if gotSQL {
	// 	sql = item.(string)
	// } else {
	model := sqlTableModel{
		Name: escapeString(knownShape.Name),
	}
	for _, p := range knownShape.Properties {
		columnModel := sqlColumnModel{
			Name: escapeString(p.Name),
		}
		for _, k := range knownShape.Keys {
			if k == p.Name {
				columnModel.IsKey = true
			}
		}
		columnModel.SqlType = convertToSQLType(p.Type, columnModel.IsKey)

		model.Columns = append(model.Columns, columnModel)
	}

	// Make sure we have the columns in a known order, for consistency
	sort.Sort(model.Columns)
	for _, c := range model.Columns {
		if !c.IsKey {
			model.NonKeyColumns = append(model.NonKeyColumns, c)
		}
	}

	// Render the SQL
	w := &bytes.Buffer{}
	err = upsertTemplate.Execute(w, model)
	if err != nil {
		return
	}

	sql = w.String()
	knownShape.Set(keyUpsertSQL, sql)
	//	}

	// item, gotOrderer = knownShape.Get(keyParameterOrderer)
	// if gotOrderer {
	// 	orderer = item.(func(pipeline.DataPoint) []interface{})
	// } else {
	orderer = func(dp pipeline.DataPoint) (p []interface{}) {
		// Populate the parameter list with values from the datapoint,
		// in the column order.
		for _, c := range model.Columns {
			value := dp.Data[c.Name]
			formattedValue := formatValue(c.SqlType, value)
			p = append(p, formattedValue)
		}

		// set the Naveego system column values as parameters
		pub, ok := datapoint.Meta["publisher"]
		if !ok {
			pub = "UNKNOWN"
		}

		pubAt, ok := datapoint.Meta["publishedAt"]
		if !ok {
			pubAt = time.Now().UTC().Format(time.RFC3339)
		}

		shapeVer, ok := datapoint.Meta["shapeVersion"]
		if !ok {
			shapeVer = "UNKNOWN"
		}

		p = append(p, formatValue("VARCHAR(1000)", pub))
		p = append(p, formatValue("DATETIME", pubAt))
		p = append(p, formatValue("VARCHAR(50)", shapeVer))

		return p
	}

	knownShape.Set(keyParameterOrderer, orderer)
	//	}

	params = orderer(datapoint)

	return
}

func formatValue(t string, value interface{}) interface{} {
	switch t {
	case "DATETIME":
		if dateString, ok := value.(string); ok {
			if date, err := time.Parse(time.RFC3339, dateString); err == nil {
				return date.UTC().Format(MySQLTimeFormat)
			}
		}
	case "TEXT":
		if valueString, ok := value.(string); ok {
			return valueString
		}
	}

	if strings.HasPrefix(t, "VARCHAR(") {
		if valueString, ok := value.(string); ok {
			sizeStr := strings.TrimRight(strings.TrimPrefix(t, "VARCHAR("), ")")
			size, err := strconv.Atoi(sizeStr)
			if err != nil {
				size = 255
			}
			if size < len(valueString) {
				valueString = valueString[:size]
			}

			return valueString
		}
	}

	return value
}

func convertToSQLType(t string, isKey bool) string {
	switch t {
	case "date":
		return "DATETIME"
	case "integer", "int":
		return "INT(10)"
	case "float", "decimal", "double":
		return "FLOAT"
	case "bool":
		return "BIT"
	case "text":
		return "TEXT"
	}

	if isKey {
		return "VARCHAR(255)"
	}

	return "VARCHAR(1000)"
}

func convertFromSQLType(t string) string {

	text := strings.ToLower(strings.Split(t, "(")[0])

	switch text {
	case "datetime", "date", "time", "smalldatetime":
		return "date"
	case "bigint", "int", "smallint", "tinyint":
		return "integer"
	case "decimal", "float", "money", "smallmoney":
		return "float"
	case "bit":
		return "bool"
	}
	return "string"
}

var sqlCleaner = regexp.MustCompile(`[^A-z0-9_\-\. ]|` + "`")

func escapeArgs(args ...string) []interface{} {
	safeArgs := make([]interface{}, len(args))
	for i, a := range args {
		safeArgs[i] = escapeString(a)
	}
	return safeArgs
}

func escapeString(arg string) string {
	return sqlCleaner.ReplaceAllString(arg, "")
}
