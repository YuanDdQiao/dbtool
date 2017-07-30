package bsonutil

import (
	"mongorsync-1.1/common/json"
	. "mongorsync-1.1/smartystreets/goconvey/convey"
	"mongorsync-1.1/mgo.v2/bson"
	"testing"
)

func TestUndefinedValue(t *testing.T) {

	Convey("When converting JSON with undefined values", t, func() {

		Convey("works for undefined literal", func() {
			key := "key"
			jsonMap := map[string]interface{}{
				key: json.Undefined{},
			}

			err := ConvertJSONDocumentToBSON(jsonMap)
			So(err, ShouldBeNil)
			So(jsonMap[key], ShouldResemble, bson.Undefined)
		})

		Convey(`works for undefined document ('{ "$undefined": true }')`, func() {
			key := "key"
			jsonMap := map[string]interface{}{
				key: map[string]interface{}{
					"$undefined": true,
				},
			}

			err := ConvertJSONDocumentToBSON(jsonMap)
			So(err, ShouldBeNil)
			So(jsonMap[key], ShouldResemble, bson.Undefined)
		})
	})
}
