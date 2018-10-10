gcc -o mongoc_api mongoc_api.c -I/usr/local/include/libbson-1.0 -I/usr/local/include/libmongoc-1.0 -lmongoc-1.0 -lbson-1.0 -fPIC -shared -o mongoc_api.so
# gcc -o test_leak test_leak.c -I/usr/local/include/libbson-1.0 -I/usr/local/include/libmongoc-1.0 -lmongoc-1.0 -lbson-1.0
