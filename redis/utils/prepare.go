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
	wKeys := make([]string, len(args))
	for i, key := range args {
		wKeys[i] = string(key)
	}
	return wKeys, nil
}
func WriteEvenKeys(args _type.Args) ([]string, []string) {
	wKeys := make([]string, len(args)/2)
	for i := 0; i < len(wKeys); i++ {
		wKeys[i] = string(args[2*i])
	}
	return wKeys, nil
}

func ReadAllKeys(args _type.Args) ([]string, []string) {
	rKeys := make([]string, len(args))
	for i, key := range args {
		rKeys[i] = string(key)
	}
	return nil, rKeys
}

func ReadTwoKeys(args _type.Args) ([]string, []string) {
	key1, key2 := string(args[0]), string(args[1])
	return []string{key1, key2}, nil
}
