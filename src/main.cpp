#include <arrow/api.h>
#include <iostream>

int main() {
    // Initialize Arrow's memory pool
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    
    // Print a simple message to verify everything works
    std::cout << "Hello from Arrow! Memory pool initialized." << std::endl;
    
    return 0;
}
