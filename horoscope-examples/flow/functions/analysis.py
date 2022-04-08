def analysis(data):
    result = []
    data_len = len(data)
    def getSentiment(positive_probs):
        prob_len = len(positive_probs)
        is_positive = True
        for j in range(prob_len):
            if float(positive_probs[j]["positive_prob"]) > 0.95:
                continue
            else :
                is_positive = False
                break
        if is_positive:
            result.append(1)
        else:
            result.append(0)

    for i in range(data_len):
        getSentiment(data[i])
    return result