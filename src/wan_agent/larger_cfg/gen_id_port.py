base_id = 0
base_gms = 23580
base_state_transfer = 28366
base_sst = 37683
base_rdmc = 31675
base_external = 32645

num_senders = 8
num_clients = 2

for i in range(num_senders + num_clients):
    print('\n\nfor id {}:\n'.format(i))

    print('# my local id - each node should have a different id')
    print('local_id = {}'.format(base_id + i))
    print('# my local ip address')
    print('local_ip = 127.0.0.1')
    print('# derecho gms port')
    print('gms_port = {}'.format(base_gms + i))
    print('# derecho rpc port')
    print('state_transfer_port = {}'.format(base_state_transfer + i))
    print('# sst tcp port')
    print('sst_port = {}'.format(base_sst + i))
    print('# rdmc tcp port')
    print('rdmc_port = {}'.format(base_rdmc + i))
    print('# external port')
    print('external_port = {}'.format(base_external + i))