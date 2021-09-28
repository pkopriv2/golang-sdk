package sql

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type Test1 struct {
	Name  string
	Other uuid.UUID
	Time  time.Time
	Bytes []byte
	Test2 Test2
}

type Test2 struct {
	Name string `json:"name"`
}

func Test1Default(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)

	driver, err := SqlLiteDialer{}.Embed(ctx)
	if !assert.Nil(t, err) {
		return
	}

	schema1 := NewSchema("test1", 0).WithStruct(Test1{}).Build()
	if !assert.Nil(t, driver.Do(schema1.Init)) {
		return
	}

	schema2 := NewSchema("test2", 0).WithStruct(Test2{}).Build()
	if !assert.Nil(t, driver.Do(schema2.Init)) {
		return
	}

	fmt.Println(enc.Json.MustEncodeString(schema1))
	fmt.Println(enc.Json.MustEncodeString(schema2))

	type Multi struct {
		Test1 `json:"test1"`
		Test2 `json:"test2"`
	}

	orig11 := Test1{Name: "Hello11", Bytes: []byte{0}, Other: uuid.NewV1(), Time: time.Now().UTC(), Test2: Test2{"name11"}}
	orig12 := Test1{Name: "Hello12", Other: uuid.NewV1()}
	orig21 := Test2{Name: "Hello21"}
	err = driver.Do(
		Exec(
			schema1.Insert(orig11),
			schema1.Insert(orig12),
			schema2.Insert(orig21)))
	if !assert.Nil(t, err) {
		t.FailNow()
		return
	}

	var ok bool
	var act Test1
	err = driver.Do(
		QueryOne(
			schema1.SelectAs("t").Where("t.name = ?", orig11.Name),
			Struct(&act),
			&ok))
	if !assert.Nil(t, err) {
		return
	}

	fmt.Println(orig11.Bytes)
	fmt.Println(act.Bytes)
	if !assert.True(t, ok) || !assert.Equal(t, orig11, act) {
		return
	}

	//var buf []Test1
	//err = driver.Do(
	//QueryPage(
	//schema1.SelectAs("t"),
	//Slice(&buf, Struct)))
	//if !assert.Nil(t, err) || !assert.Len(t, buf, 2) {
	//return
	//}

	//assert.Equal(t, orig11, buf[0])
	//assert.Equal(t, orig12, buf[1])

	//var multi Multi
	//err = driver.Do(
	//QueryOne(
	//Select(
	//schema1.Cols().As("t1").Union(schema2.Cols().As("t2"))...).
	//From(
	//schema1.As("t1"),
	//schema2.As("t2")),
	//MultiStruct(&multi),
	//&ok,
	//))
	//if !assert.Nil(t, err) || !assert.True(t, ok) {
	//return
	//}

	//var multiBuf []Multi
	//err = driver.Do(
	//QueryPage(
	//Select(
	//schema1.Cols().As("t1").Union(schema2.Cols().As("t2"))...).
	//From(
	//schema1.As("t1"),
	//schema2.As("t2")),
	//Slice(&multiBuf, MultiStruct),
	//))
	//if !assert.Nil(t, err) || !assert.True(t, ok) {
	//return
	//}

	//fmt.Println(enc.Json.MustEncodeString(multiBuf))
}
