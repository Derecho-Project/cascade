using Derecho.Cascade;
using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using static Derecho.Cascade.CascadeClient;

public class CascadeClientCLI
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Welcome to the Cascade C# Client CLI.");
        Console.WriteLine("Loading client service ref instance...");
        CascadeClient client = new CascadeClient();
        Console.WriteLine("Done.");

        string command;
        while (true)
        {
            Console.Write("cmd> ");
            command = Console.ReadLine();
            string[] parts = command.Split(' ');
            switch (parts[0].ToLower())
            {
                case "help":
                    PrintResult("TODO");
                    break;
                case "quit":
                    Console.WriteLine("Client exits.");
                    return;
                case "get_my_id":
                    PrintResult(client.GetMyId());
                    break;
                default:
                    Console.WriteLine("Invalid command, please try again or `help` for instructions.");
                    break;
            }
        }
    }

    private static void PrintResult(string result)
    {
        Console.WriteLine("-> " + result);
    }

    private static void PrintResult(UInt32 result)
    {
        Console.WriteLine("-> " + result);
    }
}
