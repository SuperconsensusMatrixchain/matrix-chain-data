package scan_server

// 扫描器管理
// key :  node + bcnname
// 确保key是唯一的
var Scaners map[string]*Scanner

// 初始化，防止空map
func init() {
	Scaners = map[string]*Scanner{}
}

func AddScaner(key string, scanner *Scanner) {
	if _, ok := Scaners[key]; !ok {
		Scaners[key] = scanner
	}
}

func RemoveScanner(key string) {
	//停止扫描的时候主动移除
	// 如果复用scanner 可能会发生问题
	if _, ok := Scaners[key]; ok {
		delete(Scaners, key)
	}
}

func GetScanner(key string) *Scanner {
	return Scaners[key]
}

func IsExist(key string) bool {
	_, ok := Scaners[key]
	return ok
}
