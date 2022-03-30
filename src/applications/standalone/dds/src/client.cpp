#include <cascade_dds/dds.hpp>
#include <iostream>
#include <vector>
#include <functional>
#include <string>
#include <readline/readline.h>
#include <readline/history.h>
#include <sys/prctl.h>

using namespace derecho::cascade;

static std::vector<std::string> tokenize(std::string& line, const char* delimiter) {
    std::vector<std::string> tokens;
    char line_buf[1024];
    std::strcpy(line_buf, line.c_str());
    char* token = std::strtok(line_buf, delimiter);
    while(token != nullptr) {
        tokens.push_back(std::string(token));
        token = std::strtok(NULL, delimiter);
    }
    return tokens;
}

static bool shell_is_active = true;

using command_handler_t = std::function<bool(DDSMetadataClient&,DDSClient&,const std::vector<std::string>&)>;
struct command_entry_t {
    const std::string cmd;
    const std::string desc;
    const std::string help;
    const command_handler_t handler;
};

void list_commands(const std::vector<command_entry_t>& command_list) {
    for (const auto& entry: command_list) {
        if (entry.handler) {
            std::cout << std::left << std::setw(32) << entry.cmd << "- " << entry.desc << std::endl;
        } else {
            std::cout << "### " << entry.cmd + " ###" << std::endl;
        }
    }
}

ssize_t find_command(const std::vector<command_entry_t>& command_list, const std::string& command) {
    ssize_t pos = 0;
    for(;pos < static_cast<ssize_t>(command_list.size());pos++) { 
        if (command_list.at(pos).cmd == command) {
            break;
        }
    }
    if (pos == static_cast<ssize_t>(command_list.size())) {
        pos = -1;
    }
    return pos;
}

#define CHECK_FORMAT(tks,sz) \
    if (tks.size() < sz) { \
        std::cerr << "Expecting " << sz << " arguments, but get " \
                  << tks.size() << " only." << std::endl; \
        return false; \
    }

std::unordered_map<std::string,std::unique_ptr<DDSSubscriber<std::string>>> subscribers;

std::vector<command_entry_t> commands = {
    {
        "General Commands", "", "", command_handler_t()
    },
    {
        "help",
        "Print help info",
        "help [command name]",
        [](DDSMetadataClient&,DDSClient&,const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() >= 2) {
                ssize_t command_index = find_command(commands,cmd_tokens[1]);
                if (command_index < 0) {
                    std::cout << "unknown command:" << cmd_tokens[1] << std::endl;
                } else {
                    std::cout << commands.at(command_index).help << std::endl;
                }
                return (command_index>=0);
            } else {
                list_commands(commands);
                return true;
            }
        }
    },
    {
        "quit",
        "Quit DDS Client",
        "quit",
        [](DDSMetadataClient&,DDSClient&,const std::vector<std::string>& cmd_tokens) {
            shell_is_active = false;
            return true;
        }
    },
    {
        "DDS Metadata Commands", "", "", command_handler_t()
    },
    {
        "list_topics",
        "List topics in the dds service",
        "list_topics",
        [](DDSMetadataClient& metadata_client,DDSClient&,const std::vector<std::string>& cmd_tokens) {
            metadata_client.template list_topics<void>(
                    [](const std::unordered_map<std::string,Topic>& topics)->void {
                        uint32_t idx = 0;
                        for (const auto& topic: topics) {
                            idx ++;
                            std::cout << "TOPIC-" << idx << std::endl;
                            std::cout << "\tname:" << topic.second.name << std::endl;
                            std::cout << "\tpath:" << topic.second.pathname << std::endl;
                        }
                    }
            );
            return true;
        }
    },
    {
        "create_topic",
        "Create a topic",
        "create_topic <topic_name> <object_pool_pathname>",
        [](DDSMetadataClient& metadata_client,DDSClient&,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            Topic topic(cmd_tokens[1],cmd_tokens[2]);
            try {
                metadata_client.create_topic(topic);
            } catch (derecho::derecho_exception& ex){
                std::cerr << "Exception:" << ex.what() << std::endl;
                return false;
            } catch (...) {
                std::cerr << "Unknown Exception Caught." << std::endl;
                return false;
            }
            return true;
        }
    },
    {
        "remove_topic",
        "Remove a topic",
        "remove_topic <topic_name>",
        [](DDSMetadataClient& metadata_client,DDSClient&,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            try {
                metadata_client.remove_topic(cmd_tokens[1]);
            } catch (derecho::derecho_exception& ex) {
                std::cerr << "Exception:" << ex.what() << std::endl;
                return false;
            } catch (...) {
                std::cerr << "Unknown Exception Caught." << std::endl;
                return false;
            }
            return true;
        }
    },
    {
        "Pub/Sub Commands", "", "", command_handler_t()
    },
    {
        "publish",
        "Publish to a topic (with predefined messages: Message #N in topic XXX)",
        "publish <topic_name> <number_of_message>",
        [](DDSMetadataClient& metadata_client,DDSClient& client,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            uint32_t num_messages = std::stol(cmd_tokens[2]);
            try{
                auto publisher = client.template create_publisher<std::string>(cmd_tokens[1]);
                if (!publisher) {
                    std::cerr << "failed to create publisher for topic:" << cmd_tokens[1] << std::endl;
                    return false;
                }
                std::cout << "publisher created for topic:" << publisher->get_topic() << std::endl;;
                for (uint32_t i=0;i<num_messages;i++) {
                    const std::string msg = std::string("Message #" + std::to_string(i) + " in topic " + publisher->get_topic());
                    dbg_default_trace("publishing msg #{} to topic:{}", i, publisher->get_topic());
                    publisher->send(msg);
                }
            } catch (derecho::derecho_exception& ex) {
                std::cerr << "Exception" << ex.what() << std::endl;
                return false;
            } catch (...) {
                std::cerr << "Uknown exception caught." << std::endl;
                return false;
            }
            return true;
        }
    },
    {
        "subscribe",
        "Subscribe to a topic",
        "subscribe <topic_name> <subscriber_name>",
        [](DDSMetadataClient& metadata_client,DDSClient& client,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            const std::string& topic = cmd_tokens[1];
            const std::string& sname = cmd_tokens[2];
            try {
                auto subscriber = client.template subscribe<std::string>(
                        topic,
                        std::unordered_map<std::string,message_handler_t<std::string>>{{
                            std::string{"default"},
                            [topic](const std::string& msg){
                                std::cout << "Message of " << msg.size() << " bytes"
                                          << " received in topic '" << topic << "': " << msg << std::endl;
                            }
                        }}
                );
                // keep trace of it
                subscribers.emplace(sname,std::move(subscriber));
            } catch (derecho::derecho_exception& ex) {
                std::cerr << "Exception" << ex.what() << std::endl;
                return false;
            } catch (...) {
                std::cerr << "Uknown exception caught." << std::endl;
                return false;
            }
            return true;
        }
    },
    {
        "unsubscribe",
        "unsubscribe from a topic",
        "unsubscribe <subscriber_name>",
        [](DDSMetadataClient& metadata_client,DDSClient& client,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            const std::string& sname = cmd_tokens[1];
            try {
                if (subscribers.find(sname) == subscribers.cend()) {
                    std::cerr << "No subscriber with name '" << sname << " is found." << std::endl;
                    return false;
                }
                client.unsubscribe(subscribers.at(sname));
                subscribers.erase(sname);
            } catch (derecho::derecho_exception& ex) {
                std::cerr << "Exception" << ex.what() << std::endl;
                return false;
            } catch (...) {
                std::cerr << "Uknown exception caught." << std::endl;
                return false;
            }
            return true;
        }
    },
    {
        "list_subscribers",
        "list current subscribers",
        "list_subscribers",
        [](DDSMetadataClient& metadata_client,DDSClient& client,const std::vector<std::string>& cmd_tokens) {
            std::cout << subscribers.size() << " subscribers found" << std::endl;
            std::cout << "NAME\tTOPIC" << std::endl;
            std::cout << "=============" << std::endl;
            for (const auto& pair:subscribers) {
                std::cout << pair.first << "\t" << pair.second->get_topic() << std::endl;
            }
            return true;
        }
    },
};

static void do_command(
        DDSMetadataClient& metadata_client,
        DDSClient& client,
        const std::vector<std::string>& cmd_tokens) {
    try {
        ssize_t command_index = find_command(commands, cmd_tokens[0]);
        if (command_index>=0) {
            if (commands.at(command_index).handler(metadata_client,client,cmd_tokens)) {
                std::cout << "-> Succeeded." << std::endl;
            } else {
                std::cout << "-> Failed." << std::endl;
            }
        } else {
            std::cout << "Unknown command:" << cmd_tokens[0] << std::endl;
        }
    } catch (const derecho::derecho_exception& ex) {
        std::cout << "Exception:" << ex.what() << std::endl;
    } catch (...) {
        std::cout << "Unknown exception caught." << std::endl;
    }
}

int main(int, char**) {
    std::cout << "Cascade DDS Client" << std::endl;
    // TODO: load dds.json
    std::shared_ptr<ServiceClientAPI> capi = std::make_shared<ServiceClientAPI>();
    auto dds_config = DDSConfig::get();
    auto metadata_client = DDSMetadataClient::create(capi,dds_config);
    auto client = DDSClient::create(capi,dds_config);

    while (shell_is_active) {
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if (cmdline == "") continue;
        add_history(cmdline.c_str());

        std::string delimiter = " ";
        do_command(*metadata_client,*client,tokenize(cmdline, delimiter.c_str()));
    }
    return 0;
}
