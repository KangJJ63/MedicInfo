#-*- coding: utf-8-*-
def query_seperator(fname, param='', debug=False):
    """
    파라메터별 쿼리 분리
    :param fname: 쿼리 파일
    :param param: 파라메터 len 2~3
    :param debug: 파라메터 제거 여부
    :return:
    """

    sep_chars_length = len(param)
    sep_query = []

    with open(fname, encoding='utf-8') as f:
        for line in f:
            line = line.rstrip()  # \r\n 중복 제거

            flag = False  # pass flag
            chars = line[:sep_chars_length].strip()

            if chars and chars.isnumeric():
                for i, c in enumerate(chars):
                    if not int(c) or c == param[i]:
                        continue

                    if not c == param[i]:
                        flag = True
                        break

                line = '   ' + line[sep_chars_length:]

            if not flag:
                sep_query.append(line)

    # not allowed ';' charactor
    return '\n'.join(sep_query).strip().rstrip(';')


if __name__ == '__main__':
    fname = [
        # 'd:/hira/query/sel_whlpay_dashbd.sql',
        # 'd:/hira/query/sel_whlpay_itm_mm.sql',
        # 'd:/hira/query/sel_whlpay_adm_mm.sql',
        # 'd:/hira/query/sel_cvrn_dashbd.sql',
        'd:/hira/query/sel_cvrn_diag.sql',
        # 'd:/hira/query/sel_cvrn_diag_div.sql',
        # 'd:/hira/query/sel_whlpay_qq.sql'
        # 'd:/hira/query/sel_cvrn_diag_st5.sql',
        # 'd:/hira/query/sel_whlpay_dashbd.sql',

        # 'd:/hira/query/sel_item_var.sql',
        # 'd:/hira/query/sel_cvrn_diag_pattern.sql',
    ]
    print(fname[-1])

    # q = query_seperator(fname[-1], '10', debug=False) % (201807, 201807)
    q = query_seperator(fname[-1], '20', debug=False) % 201810
    # q = query_seperator(fname[-1], '310')
    print(q)
