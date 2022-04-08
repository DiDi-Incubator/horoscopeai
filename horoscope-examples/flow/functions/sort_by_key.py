def sort_by_key(files):
    def sort_key(s):
        if s:
            try:
                arrays = s.split("-")
                c = arrays[0][4:] + arrays[1] + arrays[2] + arrays[3][0:2]
            except:
                c = -1
            return int(c)

    dic = {}
    for item in files:
       dic[item]= sort_key(item)

    res = []
    for (key, value) in sorted(dic.items(), key=lambda k: k[0]):
        res.append(key)
    return res
