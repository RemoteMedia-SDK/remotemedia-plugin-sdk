// Force instantiation of libc++ iostream templates on Android.
//
// Some Android plugins depend on `std::ifstream` / `std::fstream` symbols
// that are only emitted when the corresponding templates are actually
// instantiated. This helper is explicitly called during plugin root module
// initialization to ensure those symbols are present before `dlopen`.
#include <fstream>
#include <sstream>

template class std::basic_filebuf<char>;
template class std::basic_ifstream<char>;
template class std::basic_ofstream<char>;
template class std::basic_fstream<char>;
template class std::basic_istringstream<char>;
template class std::basic_ostringstream<char>;
template class std::basic_stringstream<char>;

extern "C" void loadable_node_android_force_libcxx_streams() {
  std::filebuf file_buffer;
  std::fstream file_stream;
  std::ifstream input_file_stream;
  std::ofstream output_file_stream;
  std::stringstream stream;
  stream << "";
  output_file_stream << "";
  (void)stream.str();
  (void)file_buffer.is_open();
  (void)file_stream.is_open();
  (void)input_file_stream.is_open();
  (void)output_file_stream.is_open();
}
