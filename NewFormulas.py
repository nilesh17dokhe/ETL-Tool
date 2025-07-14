def replace_(dataframe, place, reg, old_word, new_word, column_list=None):
    try:
        if column_list is None:
            column_list = []
        place_list = ['AllCol', 'MultipleCol']
        reg_list = ['StartsWith', 'EndsWith', 'InBetween', 'Exact']

        if place in place_list and reg in reg_list:
            if place == 'AllCol' and len(column_list) == 0:
                if reg == 'Exact':
                    dataframe.replace(old_word, new_word, inplace=True)
                    return dataframe
                elif reg == 'StartsWith':
                    dataframe.replace('^' + old_word, new_word, regex=True, inplace=True)
                    return dataframe
                elif reg == 'EndsWith':
                    dataframe.replace(old_word + '$', new_word, regex=True, inplace=True)
                    return dataframe
                elif reg == 'InBetween':
                    dataframe.replace(old_word, new_word, regex=True, inplace=True)
                    return dataframe
            elif place == 'MultipleCol' and len(column_list) > 0:
                # df=df[column_list]
                if reg == 'Exact':
                    dataframe[column_list] = dataframe[column_list].replace(old_word, new_word)
                    # print('Yes')
                    return dataframe
                elif reg == 'StartsWith':
                    dataframe[column_list] = dataframe[column_list].replace('^' + old_word, new_word, regex=True)
                    return dataframe
                elif reg == 'EndsWith':
                    dataframe[column_list] = dataframe[column_list].replace(old_word + '$', new_word, regex=True)
                    return dataframe
                elif reg == 'InBetween':
                    dataframe[column_list] = dataframe[column_list].replace(old_word, new_word, regex=True)
                    return dataframe
        else:
            return 'Try Some Valid Replacement!'
    except Exception as ex:
        raise Exception(str(ex))




