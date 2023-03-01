package utils

import _type "go-redis/interface/type"

func ReadFirst(args _type.Args) ([]string, []string) {
	key := string(args[0])
	return nil, []string{key}
}

func ReadFirstTwo(args _type.Args) ([]string, []string) {
	key1, key2 := string(args[0]), string(args[1])
	return []string{key1, key2}, nil
}

func ReadAll(args _type.Args) ([]string, []string) {
	rKeys := make([]string, len(args))
	for i, key := range args {
		rKeys[i] = string(key)
	}
	return nil, rKeys
}

func WriteFirst(args _type.Args) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func WriteAll(args _type.Args) ([]string, []string) {
	wKeys := make([]string, len(args))
	for i, key := range args {
		wKeys[i] = string(key)
	}
	return wKeys, nil
}
func WriteEven(args _type.Args) ([]string, []string) {
	wKeys := make([]string, len(args)/2)
	for i := 0; i < len(wKeys); i++ {
		wKeys[i] = string(args[2*i])
	}
	return wKeys, nil
}

func WriteFirstReadSecond(args _type.Args) ([]string, []string) {
	wKeys := []string{string(args[0])}
	rKeys := []string{string(args[1])}
	return wKeys, rKeys
}

func WriteFirstReadOthers(args _type.Args) ([]string, []string) {
	wKeys := []string{string(args[0])}
	rKeys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		rKeys[i] = string(args[i+1])
	}
	return wKeys, rKeys
}

func WriteNilReadNil(args _type.Args) ([]string, []string) {
	return nil, nil
}
