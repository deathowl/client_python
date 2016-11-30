import collections

def format_labels(labels):
    result = [(k, v.replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"').encode('utf-8'))
              for k, v in sorted(labels.items())]
    return result
