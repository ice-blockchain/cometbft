package helpers

import "reflect"

func ReflectTypeName(obj any) string {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t.Name()
}
