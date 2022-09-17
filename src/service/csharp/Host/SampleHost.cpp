#include <iostream>
#include "GatewayToManaged.h"

using namespace std;

// This files contains callback to be called from managed code
#include "callback.c"

int main(int argc, char* argv[])
{
	cout << "Host started" << endl;
	cout << "To quit please insert any char and press <Enter>" << endl << endl;

	GatewayToManaged gateway;
	gateway.Init(argv[0]);

	string args = "";

	string retStr = gateway.Invoke("GetDevice", args.c_str(), UnmanagedCallback);
	cout << retStr.c_str() << endl;

	retStr = gateway.Invoke("SubscribeForDevice", args.c_str(), UnmanagedCallback);
	cout << retStr.c_str() << endl;

	char ch;
	cin >> ch;

	retStr = gateway.Invoke("UnsubscribeFromDevice", args.c_str(), NULL);
	cout << retStr.c_str() << endl;

	gateway.Close();

	return 0;
}

