base_id = 0
base_port = 35000


for i in range(8):
    print('\n\nfor id {}:\n'.format(i))

    print('    "private_port": {},'.format(base_port + i))
    print('    "local_site_id": {},'.format(base_id + i))
