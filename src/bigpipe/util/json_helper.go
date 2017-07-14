package util

// 解析map中的string
func JsonGetString(dict *map[string]interface{}, key string) (string, bool){
	if iVal, keyExsit := (*dict)[key]; keyExsit {
		if val, isString := iVal.(string); isString {
			return val, true
		} else {
			return "", false
		}
	}
	return "", false
}

// 解析map中的int
func JsonGetInt(dict *map[string]interface{}, key string) (int, bool) {
	if iVal, keyExsit := (*dict)[key]; keyExsit {
		if val, isInt := iVal.(float64); isInt {
			return int(val), true
		} else {
			return 0, false
		}
	}
	return 0, false
}