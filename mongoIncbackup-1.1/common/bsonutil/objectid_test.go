package bsonutil

import (
	"mongoIncbackup-1.1/common/json"
	. "mongoIncbackup-1.1/smartystreets/goconvey/convey"
	"mongoIncbackup-1.1/mgo.v2/bson"
	"testing"
)

func TestObjectIdValue(t *testing.T) {

	Convey("When converting JSON with ObjectId values", t, func() {

		Convey("works for ObjectId constructor", func() {
			key := "key"
			jsonMap := map[string]interface{}{
				key: json.ObjectId("0123456789abcdef01234567"),
			}

			err := ConvertJSONDocumentToBSON(jsonMap)
			So(err, ShouldBeNil)
			So(jsonMap[key], ShouldEqual, bson.ObjectIdHex("0123456789abcdef01234567"))
		})

		Convey(`works for ObjectId document ('{ "$oid": "0123456789abcdef01234567" }')`, func() {
			key := "key"
			jsonMap := map[string]interface{}{
				key: map[string]interface{}{
					"$oid": "0123456789abcdef01234567",
				},
			}

			err := ConvertJSONDocumentToBSON(jsonMap)
			So(err, ShouldBeNil)
			So(jsonMap[key], ShouldEqual, bson.ObjectIdHex("0123456789abcdef01234567"))
		})
	})
}
