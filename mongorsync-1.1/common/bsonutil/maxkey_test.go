package bsonutil

import (
	"mongorsync-1.1/common/json"
	. "mongorsync-1.1/smartystreets/goconvey/convey"
	"mongorsync-1.1/mgo.v2/bson"
	"testing"
)

func TestMaxKeyValue(t *testing.T) {

	Convey("When converting JSON with MaxKey values", t, func() {

		Convey("works for MaxKey literal", func() {
			key := "key"
			jsonMap := map[string]interface{}{
				key: json.MaxKey{},
			}

			err := ConvertJSONDocumentToBSON(jsonMap)
			So(err, ShouldBeNil)
			So(jsonMap[key], ShouldResemble, bson.MaxKey)
		})

		Convey(`works for MaxKey document ('{ "$maxKey": 1 }')`, func() {
			key := "key"
			jsonMap := map[string]interface{}{
				key: map[string]interface{}{
					"$maxKey": 1,
				},
			}

			err := ConvertJSONDocumentToBSON(jsonMap)
			So(err, ShouldBeNil)
			So(jsonMap[key], ShouldResemble, bson.MaxKey)
		})
	})
}
