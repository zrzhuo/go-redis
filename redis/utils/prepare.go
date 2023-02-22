package utils

import _type "go-redis/interface/type"

/*----- keysFind: func(args _type.Args) ([]string, []string) -----*/

func ReadFirstKey(args _type.Args) ([]string, []string) {
	key := string(args[0])
	return nil, []string{key}
}

func WriteFirstKey(args _type.Args) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func WriteAllKeys(args _type.Args) ([]string, []string) {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	return keys, nil
}

func ReadAllKeys(args _type.Args) ([]string, []string) {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	return nil, keys
}
