import json







#Example getrawmempooldata RPC call

# {'0de76e865e9822c5d9e0bb612298914b336c7b3d194cf71a8f4fcb399d293c53': {'vsize': 140,
#   'weight': 559,
#   'time': 1688148387,
#   'height': 796595,
#   'descendantcount': 1,
#   'descendantsize': 140,
#   'ancestorcount': 2,
#   'ancestorsize': 1312,
#   'wtxid': '5c665b0db91942832320c1f0075a99a6ed5d4f6c57a32cb27422cc99add10690',
#   'fees': {'base': 1.232e-05,
#    'modified': 1.232e-05,
#    'ancestor': 0.00011752,
#    'descendant': 1.232e-05},
#   'depends': ['03025bb9678c53f064553b5a38a50cdcba70b342fb804c06b91878140b89aba2'],
#   'spentby': [],
#   'bip125-replaceable': True,
#   'unbroadcast': False,

def print_dictionary_structure(dictionary, indent=1):
    for key, value in dictionary.items():
        print(' ' * indent + str(key))
        if isinstance(value, dict):
            print_dictionary_structure(value, indent + 2)


def transform_getrawmempooldata(response):
    d = json.loads(response.text)['result']    
    return(d)



