package commands

import _type "go-redis/interface/type"

/* ---- Prepare: func(args _type.Args) ([]string, []string) ----- */

func readFirstKey(args _type.Args) ([]string, []string) {
	key := string(args[0])
	return nil, []string{key}
}

func writeFirstKey(args _type.Args) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func writeAllKeys(args _type.Args) ([]string, []string) {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	return keys, nil
}
