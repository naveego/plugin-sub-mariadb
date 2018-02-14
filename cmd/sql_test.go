package cmd

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/naveego/api/types/pipeline"

	"github.com/naveego/pipeline-subscribers/shapeutils"
	. "github.com/smartystreets/goconvey/convey"
)

var longText = `
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
Venenatis a condimentum vitae sapien pellentesque habitant morbi tristique. Dui id ornare arcu odio ut sem nulla pharetra diam. 
Aenean sed adipiscing diam donec adipiscing. Tincidunt praesent semper feugiat nibh. Id semper risus in hendrerit gravida rutrum 
quisque. Nisi scelerisque eu ultrices vitae auctor eu. Phasellus vestibulum lorem sed risus ultricies tristique nulla. Amet 
venenatis urna cursus eget nunc scelerisque. Urna id volutpat lacus laoreet non curabitur gravida. Venenatis urna cursus eget 
nunc scelerisque viverra mauris. Etiam erat velit scelerisque in dictum non consectetur a erat. Sollicitudin tempor id eu nisl. 
Potenti nullam ac tortor vitae purus faucibus ornare suspendisse sed. Non enim praesent elementum facilisis leo vel fringilla. 
Tempus urna et pharetra pharetra massa massa ultricies. Pulvinar pellentesque habitant morbi tristique senectus.

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
Venenatis a condimentum vitae sapien pellentesque habitant morbi tristique. Dui id ornare arcu odio ut sem nulla pharetra diam. 
Aenean sed adipiscing diam donec adipiscing. Tincidunt praesent semper feugiat nibh. Id semper risus in hendrerit gravida rutrum 
quisque. Nisi scelerisque eu ultrices vitae auctor eu. Phasellus vestibulum lorem sed risus ultricies tristique nulla. Amet 
venenatis urna cursus eget nunc scelerisque. Urna id volutpat lacus laoreet non curabitur gravida. Venenatis urna cursus eget 
nunc scelerisque viverra mauris. Etiam erat velit scelerisque in dictum non consectetur a erat. Sollicitudin tempor id eu nisl. 
Potenti nullam ac tortor vitae purus faucibus ornare suspendisse sed. Non enim praesent elementum facilisis leo vel fringilla. 
Tempus urna et pharetra pharetra massa massa ultricies. Pulvinar pellentesque habitant morbi tristique senectus.
`

func TestSafeFormat(t *testing.T) {

	execute := func(expected, input string, args ...string) {
		So(fmt.Sprintf(input, escapeArgs(args...)...), ShouldEqual, expected)
	}

	Convey("Should strip invalid chars", t, func() {
		execute("CREATE TABLE `DROP Database`", "CREATE TABLE `%s`", "`DROP Database")
		execute("CREATE TABLE `x.y`", "CREATE TABLE `%s`", "x.y")
	})

}

func TestCreateShapeChangeSQL(t *testing.T) {

	Convey("Given a shape", t, func() {

		shape := shapeutils.ShapeDelta{
			IsNew:   true,
			Name:    "test",
			NewKeys: []string{"id", "sku"},
			NewProperties: map[string]string{
				"id":   "integer",
				"date": "date",
				"str":  "string",
				"sku":  "string",
			},
		}

		Convey("When the shape is new", func() {
			shape.IsNew = true

			Convey("Then the SQL should be a CREATE statement", nil)

			actual, err := createShapeChangeSQL(shape)
			So(err, ShouldBeNil)
			So(actual, ShouldEqual, e(`CREATE TABLE IF NOT EXISTS "test" (
	"date" DATETIME NULL,
	"id" INT(10) NOT NULL,
	"sku" VARCHAR(255) NOT NULL,
	"str" VARCHAR(1000) NULL,
	"naveegoPublisher" VARCHAR(1000) DEFAULT NULL,
	"naveegoPublishedAt" DATETIME DEFAULT NULL,
	"naveegoCreatedAt" DATETIME DEFAULT CURRENT_TIMESTAMP,
	"naveegoShapeVersion" VARCHAR(50) DEFAULT NULL,
	PRIMARY KEY ("id", "sku")
)`))

		})

		Convey("When the shape is not new", func() {
			shape.IsNew = false

			Convey("When there are new keys", func() {
				shape.HasKeyChanges = true
				Convey("The the SQL should be an ALTER statement", nil)
				actual, err := createShapeChangeSQL(shape)
				So(err, ShouldBeNil)
				So(actual, ShouldEqual, e(`ALTER TABLE "test"
	ADD COLUMN IF NOT EXISTS "date" DATETIME NULL
	,ADD COLUMN IF NOT EXISTS "id" INT(10) NOT NULL
	,ADD COLUMN IF NOT EXISTS "sku" VARCHAR(255) NOT NULL
	,ADD COLUMN IF NOT EXISTS "str" VARCHAR(1000) NULL
	,DROP PRIMARY KEY
	,ADD PRIMARY KEY ("id", "sku");`))

			})

			Convey("When there are not new keys", func() {
				Convey("The the SQL should be an ALTER statement", nil)
				actual, err := createShapeChangeSQL(shape)
				So(err, ShouldBeNil)
				So(actual, ShouldEqual, e(`ALTER TABLE "test"
	ADD COLUMN IF NOT EXISTS "date" DATETIME NULL
	,ADD COLUMN IF NOT EXISTS "id" INT(10) NOT NULL
	,ADD COLUMN IF NOT EXISTS "sku" VARCHAR(255) NOT NULL
	,ADD COLUMN IF NOT EXISTS "str" VARCHAR(1000) NULL;`))
			})

		})
	})

}

func TestCreateUpsertSQL(t *testing.T) {

	Convey("Given a datapoint and a known shape", t, func() {

		longKeyValue := ""
		for index := 0; index < 32; index++ {
			longKeyValue = longKeyValue + "12345678"
		}
		expectedLongKeyValue := longKeyValue[:255]

		dp := pipeline.DataPoint{
			Entity: "Products",
			Source: "Test",
			Shape: pipeline.Shape{
				KeyNames:   []string{"ID", "LongKey"},
				Properties: []string{"NextDateAvailable:date", "ID:integer", "Name:string", "Price:float", "LongKey:string", "LongText:text"},
			},
			Data: map[string]interface{}{
				"Name":              "First",
				"Price":             42.2,
				"ID":                1,
				"NextDateAvailable": "2017-10-11",
				"LongKey":           longKeyValue,
				"LongText":          longText,
			},
		}

		shape := shapeutils.NewKnownShape(dp)

		Convey("When we generate upsert SQL for the first time", func() {
			nowDateStr := time.Now().UTC().Format("2006-01-02")
			actual, params, err := createUpsertSQL(dp, shape)
			Convey("Then there should be no error", nil)
			So(err, ShouldBeNil)
			Convey("Then the SQL should be correct", nil)
			So(actual, ShouldEqual, e(`INSERT INTO "Test.Products" ("ID", "LongKey", "LongText", "Name", "NextDateAvailable", "Price", 
	"naveegoPublisher", "naveegoPublishedAt", "naveegoShapeVersion")
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		"LongText" = VALUES("LongText"),
		"Name" = VALUES("Name"),
		"NextDateAvailable" = VALUES("NextDateAvailable"),
		"Price" = VALUES("Price"),
		"naveegoPublisher" = VALUES("naveegoPublisher"),
		"naveegoPublishedAt" = VALUES("naveegoPublishedAt"),
		"naveegoShapeVersion" = VALUES("naveegoShapeVersion");`))
			Convey("Then the parameters should be in the correct order", nil)
			So(params[0], ShouldEqual, 1)
			So(params[1], ShouldEqual, expectedLongKeyValue)
			So(params[2], ShouldEqual, longText)
			So(params[3], ShouldEqual, "First")
			So(params[4], ShouldEqual, "2017-10-11")
			So(params[5], ShouldEqual, 42.2)
			So(params[6], ShouldEqual, "UNKNOWN")
			So(params[7], ShouldStartWith, nowDateStr)
			So(params[8], ShouldEqual, "UNKNOWN")

			// Convey("Then the cache should be populated", func() {
			// 	_, ok := shape.Get(keyUpsertSQL)
			// 	So(ok, ShouldBeTrue)
			// 	_, ok = shape.Get(keyParameterOrderer)
			// 	So(ok, ShouldBeTrue)
			// })
		})

		// Convey("When we generate upsert SQL on a shape we've seen before", func() {
		// 	expectedParameters := []interface{}{"ok"}
		// 	expectedSQL := "OK"
		// 	shape.Set(keyUpsertSQL, expectedSQL)
		// 	shape.Set(keyParameterOrderer, func(datapoint pipeline.DataPoint) []interface{} {
		// 		return expectedParameters
		// 	})

		// 	actual, params, err := createUpsertSQL(dp, shape)
		// 	Convey("Then there should be no error", nil)
		// 	So(err, ShouldBeNil)
		// 	Convey("Then the cached SQL should be reused", nil)
		// 	So(actual, ShouldEqual, expectedSQL)
		// 	Convey("Then the cache parameter orderer should be used", nil)
		// 	So(params, ShouldResemble, expectedParameters)
		// })

	})
}

func Test_ConvertFromSqlType(t *testing.T) {

	Convey("Should convert correctly", t, func() {
		So(convertFromSQLType("datetime"), ShouldEqual, "date")
		So(convertFromSQLType("bigint"), ShouldEqual, "integer")
		So(convertFromSQLType("float"), ShouldEqual, "float")
		So(convertFromSQLType("bit"), ShouldEqual, "bool")
		So(convertFromSQLType("text"), ShouldEqual, "string")
	})

}

func e(s string) string {
	return strings.Replace(s, `"`, "`", -1)
}
