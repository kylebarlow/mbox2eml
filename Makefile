CXX = g++
CXXFLAGS = -O3 -std=c++20 -pthread -lstdc++fs

TARGET = mbox2eml
SRC = mbox2eml.cc

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)
