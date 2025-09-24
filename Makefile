CXX = g++
# if not macOS link statically with libstdc++
ifeq ($(shell uname),Darwin)
CXXFLAGS = -O3 -std=c++20 -pthread
LDFLAGS = -lz
else
CXXFLAGS = -O3 -std=c++20 -pthread -static
LDFLAGS = -lz
endif
TARGET = mbox2eml
SRC = mbox2eml.cc

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
