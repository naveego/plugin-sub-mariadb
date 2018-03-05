package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/navigator-go/subscribers/protocol"
	"github.com/naveego/pipeline-subscribers/shapeutils"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type mariaSubscriber struct {
	db             *sql.DB // The connection to the database
	connectionInfo string
	knownShapes    shapeutils.ShapeCache
}

type settings struct {
	DataSourceName string
}

func (h *mariaSubscriber) Init(request protocol.InitRequest) (protocol.InitResponse, error) {

	var (
		response = protocol.InitResponse{}
		err      error
	)

	err = h.connect(request.Settings)

	if err != nil {
		return response, err
	}

	// Improves performance of inserts
	_, err = h.db.Exec("SET @@session.unique_checks = 0;")
	_, err = h.db.Exec("SET @@session.foreign_key_checks = 0;")
	if err != nil {
		return response, err
	}

	if err != nil {
		return response, err
	}

	response.Message = h.connectionInfo
	response.Success = true

	return response, nil
}

func (h *mariaSubscriber) Dispose(request protocol.DisposeRequest) (protocol.DisposeResponse, error) {

	if h.db == nil {
		return protocol.DisposeResponse{
			Success: true,
			Message: "Not initialized.",
		}, nil
	}

	var err error

	err = h.db.Close()
	h.db = nil

	if err != nil {
		return protocol.DisposeResponse{
			Success: true,
			Message: "Error while closing connection.",
		}, err
	}

	return protocol.DisposeResponse{
		Success: true,
		Message: "Closed connection.",
	}, nil
}

func (h *mariaSubscriber) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {

	resp, err := h.Init(protocol.InitRequest{Settings: request.Settings})

	return protocol.TestConnectionResponse{
		Message: resp.Message,
		Success: resp.Success,
	}, err
}

func (h *mariaSubscriber) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {

	var (
		response = protocol.DiscoverShapesResponse{}
		err      error
	)

	err = h.connect(request.Settings)

	if err != nil {
		return response, err
	}

	response.Shapes = h.knownShapes.GetAllShapeDefinitions()

	return response, err
}

func (h *mariaSubscriber) ReceiveDataPoint(request protocol.ReceiveShapeRequest) (protocol.ReceiveShapeResponse, error) {

	logrus.WithField("request", request).Debug("RecieveDataPoint")

	var (
		response   = protocol.ReceiveShapeResponse{}
		knownShape *shapeutils.KnownShape
		shapeDelta shapeutils.ShapeDelta
		ok         bool
	)

	if h.db == nil {
		return response, errors.New("you must call Init before sending data points")
	}

	knownShape, ok = h.knownShapes.GetKnownShape(request.DataPoint)

	if !ok && request.DataPoint.Action != pipeline.DataPointStartPublish {
		return response, errors.New("data point shape was incompatible with shape defined in start-publish data point for this batch")
	}

	if !ok {

		shapeDelta = h.knownShapes.Analyze(request.DataPoint)

		viewName := request.DataPoint.Meta["shapeName"]
		if viewName == "" {
			viewName = request.DataPoint.Entity + "_VIEW"
		}

		sqlCommand, err := createShapeChangeSQL(shapeDelta, viewName)
		if err != nil {
			return response, err
		}

		logrus.WithField("sql", sqlCommand).Debug("Updating table")

		_, err = h.db.Exec(sqlCommand)

		if err != nil {
			logrus.WithField("request", request).WithError(err).WithField("sql", sqlCommand).Error("Error executing command")
			return response, err
		}

		knownShape = h.knownShapes.ApplyDelta(shapeDelta)
	}

	if request.DataPoint.Action == pipeline.DataPointUpsert {

		upsertCommand, upsertParameters, err := createUpsertSQL(request.DataPoint, knownShape)
		if err != nil {
			return response, err
		}

		logrus.WithFields(logrus.Fields{"sql": upsertCommand, "params": upsertParameters}).Debug("Upserting record")

		_, err = h.db.Exec(upsertCommand, upsertParameters...)

		if err != nil {
			logrus.WithField("request", request).WithError(err).WithField("sql", upsertCommand).WithField("parameters", upsertParameters).Error("Error executing upsert")

			return protocol.ReceiveShapeResponse{
				Success: false,
			}, err
		}

	}

	return protocol.ReceiveShapeResponse{
		Success: true,
	}, nil
}

func (h *mariaSubscriber) connect(settingsMap map[string]interface{}) error {

	// If we already connected, we shouldn't do anything.
	if h.db != nil {
		return nil
	}

	var (
		settings = &settings{}
		err      error
		version  string
		db       *sql.DB
	)

	err = mapstructure.Decode(settingsMap, settings)
	if err != nil {
		return fmt.Errorf("couldn't decode settings: %s", err)
	}

	if settings.DataSourceName == "" {
		return errors.New("settings didn't contain DataSourceName key")
	}

	db, err = sql.Open("mysql", settings.DataSourceName)

	if err != nil {
		return fmt.Errorf("couldn't open SQL connection: %s", err)
	}

	db.QueryRow("SELECT VERSION()").Scan(&version)

	if len(version) == 0 {
		return fmt.Errorf("couldn't get data from database server")
	}

	h.connectionInfo = fmt.Sprintf("Connected to: %s", version)
	h.db = db
	shapes, err := h.getKnownShapes()
	if err != nil {
		return err
	}

	h.knownShapes = shapeutils.NewShapeCacheWithShapes(shapes)

	return nil
}

func (s *mariaSubscriber) receiveShapeToTable(ctx subscriber.Context, shape pipeline.ShapeDefinition, dataPoint pipeline.DataPoint) error {
	schemaName := "dbo"
	tableName := shape.Name

	if strings.Contains(shape.Name, "__") {
		idx := strings.Index(shape.Name, "__")
		schemaName = tableName[:idx]
		tableName = tableName[idx+2:]
	}

	valCount := len(ctx.Pipeline.Mappings)
	vals := make([]interface{}, valCount)
	for i := 0; i < valCount; i++ {
		vals[i] = new(interface{})
	}

	colNames := []string{}
	params := []string{}
	index := 1
	for _, m := range ctx.Pipeline.Mappings {
		p := fmt.Sprintf("?%d", index)
		params = append(params, p)
		colNames = append(colNames, m.To)

		if v, ok := dataPoint.Data[m.From]; ok {
			vals[index-1] = v
		}

		index++
	}

	colNameStr := strings.Join(colNames, ",")
	paramsStr := strings.Join(params, ",")
	cmd := fmt.Sprintf("INSERT INTO [%s].[%s] (%s) VALUES (%s)", schemaName, tableName, colNameStr, paramsStr)
	logrus.Infof("Command: %s", cmd)
	_, e := s.db.Exec(cmd, vals...)
	if e != nil {
		return e
	}

	return nil
}

func (h *mariaSubscriber) getKnownShapes() (map[string]*shapeutils.KnownShape, error) {

	var (
		err        error
		rows       *sql.Rows
		tableNames []string
		shapes     = map[string]*shapeutils.KnownShape{}
	)

	rows, err = h.db.Query("SHOW TABLES")
	if err != nil {
		return shapes, err
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return shapes, err
		}
		tableNames = append(tableNames, tableName)
	}

	for _, table := range tableNames {

		rows, err = h.db.Query(fmt.Sprintf("DESCRIBE `%s`", table))
		if err != nil {
			return shapes, err
		}
		dp := pipeline.DataPoint{
			Shape: pipeline.Shape{},
		}

		for rows.Next() {
			var (
				field   string
				coltype string
				null    string
				key     string
				def     interface{}
				extra   string
			)
			err = rows.Scan(&field, &coltype, &null, &key, &def, &extra)
			if err != nil {
				return shapes, err
			}
			if strings.HasPrefix(field, "naveego") {
				continue
			}

			dp.Source = table
			if key == "PRI" {
				dp.Shape.KeyNames = append(dp.Shape.KeyNames, field)
			}
			dp.Shape.Properties = append(dp.Shape.Properties, field+":"+convertFromSQLType(coltype))
		}

		shape := shapeutils.NewKnownShape(dp)

		shapes[shape.Name] = shape
	}

	return shapes, nil
}
