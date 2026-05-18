#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <sstream>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"

int main(int argc, char *argv[]) {
    // TODO: implement the STOMP client
    StompProtocol protocol;
    ConnectionHandler* connectionHandler = nullptr;
    std::thread* socketThread = nullptr; 

    while (true) {
        const short bufsize = 1024;
        char buf[bufsize];
        std::cin.getline(buf, bufsize);
        std::string line(buf);
        std::vector<std::string> args;
        std::stringstream ss(line);
        std::string word;
        while (ss >> word) {
            args.push_back(word);
        }        
        
        if (args.empty()) continue; 

        if (args[0] == "login") {
            if (args.size() < 2) {
                std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
                continue;
            }
            std::string hostAndPort = args[1];
            size_t position = hostAndPort.find(':');
            if (protocol.isConnected()) {
                std::cout << "The client is already logged in, log out before trying again" << std::endl;
                continue;
            }
            if (position == std::string::npos){
                std::cerr << "Invalid host:port format" << std::endl;
                continue;
            }
            std::string host = hostAndPort.substr(0, position); 
            short port = (short)std::stoi(hostAndPort.substr(position + 1));
            connectionHandler = new ConnectionHandler(host, port);
            if (!connectionHandler->connect()) {
                std::cerr << "Cannot connect to " << host << ":" << port << std::endl;
                delete connectionHandler;
                connectionHandler = nullptr;
                continue;
            }
            socketThread = new std::thread([connectionHandler, &protocol]() {
                while (!protocol.shouldTerminate()) {
                    std::string frame;
                    if (!connectionHandler->getFrameAscii(frame, '\0')) { 
                        std::cout << "Disconnected from server" << std::endl;
                        protocol.setShouldTerminate(true); 
                        protocol.notifyAll(); 
                        break;
                    }
                    protocol.processServerFrame(frame);
                }
            });
            protocol.handleLogin(args, *connectionHandler);
            
        } 
        else if (args[0] == "logout") {
            if (connectionHandler) {
                protocol.handleLogout(*connectionHandler);
                if (socketThread && socketThread->joinable()) {
                    socketThread->join(); 
                    delete socketThread;
                    socketThread = nullptr;
                }
                delete connectionHandler;
                connectionHandler = nullptr;
            }
        }
        else {
            if (protocol.isConnected()) {
                if (connectionHandler) {
                    protocol.processKeyboardCommand(line, *connectionHandler);
                }
            } else {
                std::cout << "Please login first" << std::endl;
            }
        }
        if (protocol.shouldTerminate()) break;
    }
    if (socketThread && socketThread->joinable()) {
        socketThread->join();
        delete socketThread;
    }
    if (connectionHandler) {
        delete connectionHandler;
    }
    return 0;
}