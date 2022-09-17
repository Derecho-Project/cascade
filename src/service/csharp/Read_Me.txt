
// This is sample of JSON string for unmanaged Device structure sent from managed part to unmanaged one
{"type":"TypeX","id":1001,"valid":True,"properties":["yellow","white","yellow"],"value":24}


// This is command to build unmanaged host in Linux
g++ -o Host.out -D LINUX jsmn.c GatewayToManaged.cpp SampleHost.cpp -ldl
