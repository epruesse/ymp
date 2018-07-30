from ymp.helpers import odict, update_dict


def test_ordered_dict_maker():
    data = odict[
        'B': 2,
        'A': 1,
    ]
    assert data['A'] == 1
    assert data['B'] == 2
    for n, m in zip(data, ('B', 'A')):
        assert n == m


def test_update_dict():
    data = {'A': 1, 'B': 2, 'D': {'E': 3, 'F': 4}}

    assert update_dict(data, None) == data

    new_data = update_dict(data, {'C': 3})
    assert new_data['C'] == 3
    del new_data['C']
    assert new_data == data

    new_data = update_dict(data, {'D': {'E': 5, 'G': 6, 'H': {'I': 7}}})
    assert new_data['D']['E'] == 5
    assert new_data['D']['G'] == 6
    assert new_data['D']['H']['I'] == 7
