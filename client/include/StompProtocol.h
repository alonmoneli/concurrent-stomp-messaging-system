#pragma once

#include "../include/ConnectionHandler.h"
#include "../include/event.h" 
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>

class StompProtocol {
private:
    std::string username;
    std::map<std::string, int> activeSubscriptions; 
    std::map<std::string, std::map<std::string, std::vector<Event>>> gameEvents;
    bool shouldTerminate_ = false;
    bool isConnected_ = false;
    int subscriptionIdCounter = 0;
    int receiptIdCounter = 0;
    int expectedLogoutId = -1; 
    bool isLogoutProcessed = false; 
    std::mutex protocolMutex;
    std::mutex logoutMutex; 
    std::condition_variable loginCv; 
    std::condition_variable logoutCv;
    std::string extractHeader(const std::string& frame, const std::string& key);
    Event parseEventBody(const std::string& body);

public:
    StompProtocol(); 
    void processKeyboardCommand(std::string commandLine, ConnectionHandler& handler);
    void processServerFrame(std::string frame);
    bool shouldTerminate() const { return shouldTerminate_; }
    void setShouldTerminate(bool val) { shouldTerminate_ = val; }
    bool isConnected() const { return isConnected_; }  
    void notifyAll() { loginCv.notify_all(); logoutCv.notify_all(); } 
    void handleLogin(const std::vector<std::string>& args, ConnectionHandler& handler);
    void handleJoin(const std::vector<std::string>& args, ConnectionHandler& handler);
    void handleExit(const std::vector<std::string>& args, ConnectionHandler& handler);
    void handleReport(const std::vector<std::string>& args, ConnectionHandler& handler);
    void handleSummary(const std::vector<std::string>& args);
    void handleLogout(ConnectionHandler& handler);
    void handleConnected(std::string frame);
    void handleMessage(std::string frame);
    void handleReceipt(std::string frame);
    void handleError(std::string frame);
};