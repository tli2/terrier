all:
	g++ -Wall -L/usr/lib rdma.cpp client.cpp -libverbs -o client
	g++ -Wall -L/usr/lib rdma.cpp server.cpp -libverbs -o server

# gcc -O3 -Wall -L/usr/lib rdma_rc_example.c -libverbs -o rdma_rc_example