package otgorm

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"reflect"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

const (
	parentSpanGormKey = "opentracingParentSpan"
	spanGormKey       = "opentracingSpan"
)

// SetSpanToGorm sets span to gorm settings, returns cloned DB
func SetSpanToGorm(ctx context.Context, db *gorm.DB) *gorm.DB {
	if ctx == nil {
		return db
	}
	parentSpan := opentracing.SpanFromContext(ctx)
	if parentSpan == nil {
		return db
	}
	return db.Set(parentSpanGormKey, parentSpan)
}

// AddGormCallbacks adds callbacks for tracing, you should call SetSpanToGorm to make them work
func AddGormCallbacks(db *gorm.DB) {
	callbacks := newCallbacks()
	registerCallbacks(db, "create", callbacks)
	registerCallbacks(db, "query", callbacks)
	registerCallbacks(db, "update", callbacks)
	registerCallbacks(db, "delete", callbacks)
	registerCallbacks(db, "row_query", callbacks)
}

type callbacks struct{}

func newCallbacks() *callbacks {
	return &callbacks{}
}

func (c *callbacks) beforeCreate(scope *gorm.Scope)   { c.before(scope) }
func (c *callbacks) afterCreate(scope *gorm.Scope)    { c.after(scope, "INSERT") }
func (c *callbacks) beforeQuery(scope *gorm.Scope)    { c.before(scope) }
func (c *callbacks) afterQuery(scope *gorm.Scope)     { c.after(scope, "SELECT") }
func (c *callbacks) beforeUpdate(scope *gorm.Scope)   { c.before(scope) }
func (c *callbacks) afterUpdate(scope *gorm.Scope)    { c.after(scope, "UPDATE") }
func (c *callbacks) beforeDelete(scope *gorm.Scope)   { c.before(scope) }
func (c *callbacks) afterDelete(scope *gorm.Scope)    { c.after(scope, "DELETE") }
func (c *callbacks) beforeRowQuery(scope *gorm.Scope) { c.before(scope) }
func (c *callbacks) afterRowQuery(scope *gorm.Scope)  { c.after(scope, "") }

func (c *callbacks) before(scope *gorm.Scope) {
	val, ok := scope.Get(parentSpanGormKey)
	if !ok {
		return
	}
	parentSpan := val.(opentracing.Span)
	tr := parentSpan.Tracer()
	sp := tr.StartSpan("sql", opentracing.ChildOf(parentSpan.Context()))
	ext.DBType.Set(sp, scope.DB().Dialect().GetName())
	ext.DBInstance.Set(sp, scope.InstanceID())
	scope.Set(spanGormKey, sp)
}

func (c *callbacks) after(scope *gorm.Scope, operation string) {
	val, ok := scope.Get(spanGormKey)
	if !ok {
		return
	}
	sp := val.(opentracing.Span)
	if operation == "" {
		operation = strings.ToUpper(strings.Split(scope.SQL, " ")[0])
	}
	ext.Error.Set(sp, scope.HasError())
	sp.SetTag("db.table", scope.TableName())
	sp.SetTag("db.method", operation)
	sp.SetTag("db.count", scope.DB().RowsAffected)

	// set db error message tracing tag
	if scope.HasError() {
		sp.SetTag("db.err", scope.DB().Error)
	}

	// set db full statement tracing tag
	statement := setStatement(scope)
	ext.DBStatement.Set(sp, statement)

	sp.Finish()
}

func registerCallbacks(db *gorm.DB, name string, c *callbacks) {
	beforeName := fmt.Sprintf("tracing:%v_before", name)
	afterName := fmt.Sprintf("tracing:%v_after", name)
	gormCallbackName := fmt.Sprintf("gorm:%v", name)
	// gorm does some magic, if you pass CallbackProcessor here - nothing works
	switch name {
	case "create":
		db.Callback().Create().Before(gormCallbackName).Register(beforeName, c.beforeCreate)
		db.Callback().Create().After(gormCallbackName).Register(afterName, c.afterCreate)
	case "query":
		db.Callback().Query().Before(gormCallbackName).Register(beforeName, c.beforeQuery)
		db.Callback().Query().After(gormCallbackName).Register(afterName, c.afterQuery)
	case "update":
		db.Callback().Update().Before(gormCallbackName).Register(beforeName, c.beforeUpdate)
		db.Callback().Update().After(gormCallbackName).Register(afterName, c.afterUpdate)
	case "delete":
		db.Callback().Delete().Before(gormCallbackName).Register(beforeName, c.beforeDelete)
		db.Callback().Delete().After(gormCallbackName).Register(afterName, c.afterDelete)
	case "row_query":
		db.Callback().RowQuery().Before(gormCallbackName).Register(beforeName, c.beforeRowQuery)
		db.Callback().RowQuery().After(gormCallbackName).Register(afterName, c.afterRowQuery)
	}
}

func setStatement(scope *gorm.Scope) string {
	replacer := make([]string, 0)
	for i := 1; i <= len(scope.SQLVars); i++ {
		var sqlValue string

		// get value from sql vars
		val := scope.SQLVars[i-1]

		// get reflect
		ref := reflect.ValueOf(val).Kind()

		// check for reflect kind of string
		switch ref {
		case reflect.String:
			sqlValue = fmt.Sprintf(`'%s'`, val)
		case reflect.Interface:

			// set default value null
			sqlValue = "NULL"

			// check type of interface
			switch val.(type) {
			case time.Time:
				time := val.(time.Time)
				sqlValue = fmt.Sprintf(`'%v'`, time.String())
			case sql.NullTime:
				null := val.(sql.NullTime)
				if null.Valid {
					sqlValue = fmt.Sprintf(`'%v'`, null.Time.String())
				}
			case sql.NullString:
				null := val.(sql.NullString)
				if null.Valid {
					sqlValue = fmt.Sprintf(`'%v'`, null.String)
				}
			case sql.NullInt64:
				null := val.(sql.NullInt64)
				if null.Valid {
					sqlValue = fmt.Sprintf(`%v`, null.Int64)
				}
			case sql.NullInt32:
				null := val.(sql.NullInt32)
				if null.Valid {
					sqlValue = fmt.Sprintf(`%v`, null.Int32)
				}
			case sql.NullBool:
				null := val.(sql.NullBool)
				if null.Valid {
					sqlValue = fmt.Sprintf(`%v`, null.Bool)
				}
			case sql.NullFloat64:
				null := val.(sql.NullFloat64)
				if null.Valid {
					sqlValue = fmt.Sprintf(`%v`, null.Float64)
				}
			case pq.NullTime:
				null := val.(pq.NullTime)
				if null.Valid {
					sqlValue = fmt.Sprintf(`'%v'`, null.Time)
				}
			}
		default:
			sqlValue = fmt.Sprintf(`%v`, val)
		}

		// push to replacer
		replacer = append(replacer, fmt.Sprintf(`$%d`, i), sqlValue)
	}

	// replace statement
	r := strings.NewReplacer(replacer...)

	// set result
	result := r.Replace(scope.SQL)

	return result
}
