#include <iostream>
#include <cascade/service_client_api.hpp>
#include <iostream>
#include <cascade/object.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <cascade/service_client_api.hpp>

int main(int argc, char** argv) {
    std::cout << "Word Count C# Example" << std::endl;
    std::cout << "Loading Service Client API..." << std::endl;
    derecho::cascade::ServiceClientAPI& capi = derecho::cascade::ServiceClientAPI::get_service_client();

    std::cout << "Done." << std::endl;

    std::string user_input;
    while (true) {
        std::cout << "Type a sentence, or type q to quit." << std::endl;
        std::getline(std::cin, user_input);
        if (user_input == "q") {
            break;
        }
        std::cout << "You typed: " << user_input << std::endl; 

        std::size_t size = mutils::bytes_size(user_input);
        uint8_t stack_buffer[size]; 
        mutils::to_bytes(user_input, stack_buffer);
        derecho::cascade::Blob blob(stack_buffer, size, true);
        derecho::cascade::ObjectWithStringKey sentence_object("/word_count_map/obj_a", blob);
        auto result = capi.put(sentence_object);
    }
    
    return 0;
}