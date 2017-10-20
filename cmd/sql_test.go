package cmd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/naveego/api/types/pipeline"

	"github.com/naveego/pipeline-subscribers/shapeutils"
	. "github.com/smartystreets/goconvey/convey"
)

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
	"sku" VARCHAR(1000) NOT NULL,
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
	,ADD COLUMN IF NOT EXISTS "sku" VARCHAR(1000) NOT NULL
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
	,ADD COLUMN IF NOT EXISTS "sku" VARCHAR(1000) NOT NULL;`))

			})

		})
	})

}

func TestCreateUpsertSQL(t *testing.T) {

	Convey("Given a datapoint and a known shape", t, func() {

		dp := pipeline.DataPoint{
			Entity: "Products",
			Source: "Test",

			Shape: pipeline.Shape{
				KeyNames:   []string{"ID"},
				Properties: []string{"NextDateAvailable:date", "ID:integer", "Name:string", "Price:float"},
			},
			Data: map[string]interface{}{
				"Name":              "First",
				"Price":             42.2,
				"ID":                1,
				"NextDateAvailable": "2017-10-11",
			},
		}

		shape := shapeutils.NewKnownShape(dp)

		Convey("When we generate upsert SQL for the first time", func() {

			actual, params, err := createUpsertSQL(dp, shape)
			Convey("Then there should be no error", nil)
			So(err, ShouldBeNil)
			Convey("Then the SQL should be correct", nil)
			So(actual, ShouldEqual, e(`INSERT INTO "Test.Products" ("ID", "Name", "NextDateAvailable", "Price")
	VALUES (?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		"Name" = VALUES("Name")
		,"NextDateAvailable" = VALUES("NextDateAvailable")
		,"Price" = VALUES("Price");`))
			Convey("Then the parameters should be in the correct order", nil)
			So(params, ShouldResemble, []interface{}{1, "First", "2017-10-11", 42.2})

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
	})

}

func e(s string) string {
	return strings.Replace(s, `"`, "`", -1)
}
