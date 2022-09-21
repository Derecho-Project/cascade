#include <iostream>
#include <string.h>
#include <stdbool.h>
#include "jsmn.h"

#define PROP_NUM  7
#define STR_LEN  21

typedef struct 
{
	char type[STR_LEN];
	bool is_valid;
	int id;
	char properties[PROP_NUM][STR_LEN];
	int value;
} Device;

static int jsoneq(const char *json, jsmntok_t *tok, const char *s) 
{
	return (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
		    strncmp(json + tok->start, s, tok->end - tok->start) == 0) 
		? 0 : 1;
}

static void to_string1(const char* json, jsmntok_t* t, char* str) 
{
	snprintf(str, t->end - t->start + 1, "%.*s", t->end - t->start, json + t->start);
}

static void to_string(const char* json, jsmntok_t* t, int i, char* str) 
{
	to_string1(json, &(t[i + 1]), str);
}

#define ALLOC_STRING(str, n) str = (char*)malloc(n + 1); str[n] = '\0';

static bool atobool(const char* str)
{
	return strcmp(str, "true") == 0;
}

static void printDevice(Device device) 
{
	int i;
	const char* isValid = device.is_valid ? "valid" : "invalid";
	printf("Device: %s %i %s %i ", device.type, device.id, isValid, device.value);

	for (i = 0; i < PROP_NUM; i++) 
	{
		if (device.properties[i][0] != '*')
			printf("%s ", device.properties[i]);
		else
			break;
	}
	
	printf("\n");
}


// Callback function passed to managed code to facilitate calling back into native code with status
bool UnmanagedCallback(const char* actionName, const char* jsonArgs)
{
	jsmn_parser p;
	jsmntok_t t[128]; // We expect no more than 128 tokens
	
	jsmn_init(&p);
	int r = jsmn_parse(&p, jsonArgs, strlen(jsonArgs), t, sizeof(t) / sizeof(t[0]));

	Device device;

	int i, j;
	char* str;

	// Assume the top-level element is an object
	if (r < 1 || t[0].type != JSMN_OBJECT) 
	{
		printf("Object expected\n");
		return false;
	}
		
	for (i = 0; i < PROP_NUM; i++)
		for (j = 0; j < STR_LEN; j++)
			device.properties[i][j] = '*';

	// Loop over all keys of the root object 
	for (i = 1; i < r; i++) 
	{
		if (jsoneq(jsonArgs, &t[i], "type") == 0)
		{
			to_string(jsonArgs, t, i++, device.type);
			continue;
		}
	
		if (jsoneq(jsonArgs, &t[i], "id") == 0) 
		{
			ALLOC_STRING(str, STR_LEN)
			to_string(jsonArgs, t, i++, str);
			device.id = atoi(str);
			free(str);
			continue;
		}

		if (jsoneq(jsonArgs, &t[i], "valid") == 0)
		{
			ALLOC_STRING(str, STR_LEN)
				to_string(jsonArgs, t, i++, str);
			device.is_valid = atobool(str);
			free(str);
			continue;
		}
		
		if (jsoneq(jsonArgs, &t[i], "properties") == 0) 
		{
			int j;
			if (t[i + 1].type != JSMN_ARRAY) 
				continue; // We expect properties to be an array of strings

			for (j = 0; j < t[i + 1].size; j++) 
			{
				jsmntok_t *g = &t[i + j + 2];
				to_string1(jsonArgs, g, device.properties[j]);
			}

			i += t[i + 1].size + 1;
			continue;
		}

		if (jsoneq(jsonArgs, &t[i], "value") == 0)
		{
			ALLOC_STRING(str, STR_LEN)
			to_string(jsonArgs, t, i++, str);
			device.value = atoi(str);
			free(str);
			continue;
		}
		
		printf("Unexpected key: %.*s\n", t[i].end - t[i].start, jsonArgs + t[i].start);
	}

	printDevice(device);

	return true;
}

