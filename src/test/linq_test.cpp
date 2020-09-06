#include <boolinq/boolinq.h>
#include <iostream>

using namespace boolinq;

/**
 * LINQ Creators
 */
template <typename T>
Linq<std::pair<T, T>, typename std::iterator_traits<T>::value_type> from(const T& begin, const T& end) {
    return //TODO:
}

int main(int argc, char** argv) {
    std::cout << "boolinq test." << std::endl;

    std::vector<int> src = {1,2,3,4,5,6,7,8,9,10};
    auto dst = from(src).where([](int a){return a%2 == 1;}).toStdVector();
    std::cout << "type is:" << typeid(decltype(dst)).name() << std::endl;

    std::cout << "output:" << std::endl;
    for (auto x: dst) {
        std::cout << x << std::endl;
    }

    return 0;
}
