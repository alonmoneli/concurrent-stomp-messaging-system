#include "../include/StompProtocol.h"
#include <sstream>
#include <iostream>
#include <fstream>
#include <algorithm>

StompProtocol::StompProtocol() : username(""), subscriptionIdCounter(0), receiptIdCounter(0), expectedLogoutId(-1), isLogoutProcessed(false), shouldTerminate_(false), isConnected_(false) {}

std::string StompProtocol::extractHeader(const std::string& frame, const std::string& key) {
    std::string keyPattern = key + ":";
    size_t pos = frame.find(keyPattern);
    if (pos == std::string::npos) {
        return "";
    }
    size_t endPos = frame.find('\n', pos);
    if (endPos == std::string::npos) {
        endPos = frame.length();
    }
    std::string value = frame.substr(pos + keyPattern.length(), endPos - (pos + keyPattern.length()));
    if (!value.empty() && value.back() == '\r') {
        value.pop_back();
    }
    return value;
}

Event StompProtocol::parseEventBody(const std::string& body) {
    std::stringstream ss(body);
    std::string line;
    std::string teamA, teamB, eventName, description, user;
    int time = 0;
    std::map<std::string, std::string> gameUpdates, teamAUpdates, teamBUpdates;
    std::string currentSection = "";
    
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back(); 
        
        if (line.find("user:") == 0) user = line.substr(5);
        else if (line.find("team a:") == 0) teamA = line.substr(7);
        else if (line.find("team b:") == 0) teamB = line.substr(7);
        else if (line.find("event name:") == 0) eventName = line.substr(11);
        else if (line.find("time:") == 0) {
            try { time = std::stoi(line.substr(5)); } catch(...) { time=0; }
        }
        else if (line == "general game updates:") currentSection = "general";
        else if (line == "team a updates:") currentSection = "team_a";
        else if (line == "team b updates:") currentSection = "team_b";
        else if (line == "description:") currentSection = "description";
        else if (!line.empty()) {
            if (currentSection == "description") {
                description += line + "\n";
            } else {
                size_t splitPos = line.find(':');
                if (splitPos != std::string::npos) {
                    std::string key = line.substr(0, splitPos);
                    std::string val = line.substr(splitPos + 1);
                    if (currentSection == "general") gameUpdates[key] = val;
                    else if (currentSection == "team_a") teamAUpdates[key] = val;
                    else if (currentSection == "team_b") teamBUpdates[key] = val;
                }
            }
        }
    }
    return Event(teamA, teamB, eventName, time, gameUpdates, teamAUpdates, teamBUpdates, description);
}

void StompProtocol::processKeyboardCommand(std::string commandLine, ConnectionHandler& handler) {   
    std::stringstream ss(commandLine);
    std::string command;
    ss >> command; 
    std::vector<std::string> args;
    std::string arg;
    while (ss >> arg) {
        args.push_back(arg);
    }
    if (command == "login") {
        handleLogin(args, handler);
    } else if (command == "join") {
        handleJoin(args, handler);
    } else if (command == "exit") {
        handleExit(args, handler);
    } else if (command == "report") {
        handleReport(args, handler);
    } else if (command == "summary") {
        handleSummary(args);
    } else if (command == "logout") {
        handleLogout(handler);
    } else {
        std::cout << "Unknown command" << std::endl;
    }
}

void StompProtocol::handleLogin(const std::vector<std::string>& args, ConnectionHandler& handler){        
    if (args.size() < 4) {
        std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
        return;
    }

    this->username = args[2]; 
    std::string password = args[3];
    std::string frame = "CONNECT\n";
    frame += "accept-version:1.2\n";
    frame += "host:stomp.cs.bgu.ac.il\n"; 
    frame += "login:" + username + "\n";
    frame += "passcode:" + password + "\n";
    frame += "\n";  
    
    handler.sendFrameAscii(frame, '\0'); 
    
    std::unique_lock<std::mutex> lock(protocolMutex);
    isConnected_ = false;   
    loginCv.wait(lock, [this]{ return isConnected_ || shouldTerminate_; });
    
    if (!isConnected_) {
        std::cout << "Login failed. Please try again." << std::endl;
    }    
}

void StompProtocol::handleJoin(const std::vector<std::string>& args, ConnectionHandler& handler) {
    if (args.empty()) return;   
    std::string gameName = args[0];
    std::lock_guard<std::mutex> lock(protocolMutex); 
    int idOfSub = subscriptionIdCounter++;
    activeSubscriptions[gameName] = idOfSub;   
    int IdOfRecipet = receiptIdCounter++;  
    std::string frame = "SUBSCRIBE\n";
    frame += "destination:/" + gameName + "\n";
    frame += "id:" + std::to_string(idOfSub) + "\n";
    frame += "receipt:" + std::to_string(IdOfRecipet) + "\n\n";   

    handler.sendFrameAscii(frame, '\0');
}

void StompProtocol::handleExit(const std::vector<std::string>& args, ConnectionHandler& handler){
    if (args.empty()) return;   
    std::string gameName = args[0];
    std::lock_guard<std::mutex> lock(protocolMutex); 
    if (activeSubscriptions.find(gameName) == activeSubscriptions.end()) {
        std::cout << "Error: Not subscribed to " << gameName << std::endl;
        return;
    }
    int idOfSub = activeSubscriptions[gameName];
    activeSubscriptions.erase(gameName);   
    int IdOfRecipet = receiptIdCounter++;  
    std::string frame = "UNSUBSCRIBE\n";
    frame += "id:" + std::to_string(idOfSub) + "\n";
    frame += "receipt:" + std::to_string(IdOfRecipet) + "\n\n";
    handler.sendFrameAscii(frame, '\0');
}

void StompProtocol::handleReport(const std::vector<std::string>& args, ConnectionHandler& handler) {
    if (args.empty()) return;
    std::string file = args[0];
    names_and_events data;
    try {
        data = parseEventsFile(file); 
    } 
    catch (...) {
        std::cout << "Error parsing file." << std::endl;
        return;
    }
    std::lock_guard<std::mutex> lock(protocolMutex); 
    for (const Event& event : data.events) {
        std::string frame = "SEND\n";
        frame += "destination:/" + data.team_a_name + "_" + data.team_b_name + "\n";
        frame += "\n"; 
        frame += "user:" + username + "\n";
        frame += "team a:" + data.team_a_name + "\n";
        frame += "team b:" + data.team_b_name + "\n";
        frame += "event name:" + event.get_name() + "\n";
        frame += "time:" + std::to_string(event.get_time()) + "\n";
        frame += "general game updates:\n";
        for (auto const& [key, val] : event.get_game_updates()) {
            frame += key + ":" + val + "\n";
        }
        frame += "team a updates:\n";
        for (auto const& [key, val] : event.get_team_a_updates()) {
            frame += key + ":" + val + "\n"; 
        }
        frame += "team b updates:\n";
        for (auto const& [key, val] : event.get_team_b_updates()) {
            frame += key + ":" + val + "\n";
        }
        frame += "description:\n" + event.get_discription() + "\n";
        handler.sendFrameAscii(frame, '\0'); 
    }
}

void StompProtocol::handleSummary(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cout << "Usage: summary {game_name} {user} {file}" << std::endl;
        return;
    }
    std::string gameName = args[0];
    std::string user = args[1];
    std::string fileName = args[2];
    std::lock_guard<std::mutex> lock(protocolMutex);
    if (gameEvents.find(gameName) == gameEvents.end() || 
        gameEvents[gameName].find(user) == gameEvents[gameName].end()) {
        std::cout << "Error: No events found for user " << user << " in game " << gameName << std::endl;
        return;
    }
    const std::vector<Event>& events = gameEvents[gameName][user];
    if (events.empty()) return;
    std::map<std::string, std::string> finalGameUpdates;
    std::map<std::string, std::string> finalTeamAUpdates;
    std::map<std::string, std::string> finalTeamBUpdates;
    std::string teamA = events[0].get_team_a_name();
    std::string teamB = events[0].get_team_b_name();
    for (const auto& event : events) {
        for (auto const& [key, val] : event.get_game_updates()) {
            finalGameUpdates[key] = val;
        }
        for (auto const& [key, val] : event.get_team_a_updates()) {
            finalTeamAUpdates[key] = val;
        }
        for (auto const& [key, val] : event.get_team_b_updates()) {
            finalTeamBUpdates[key] = val;
        }
    }
    std::ofstream outFile(fileName);
    if (!outFile.is_open()) {
        std::cout << "Error: Could not open file " << fileName << std::endl;
        return;
    }
    outFile << teamA << " vs " << teamB << "\n";
    outFile << "Game stats:\n";
    outFile << "General stats:\n";
    for (auto const& [key, val] : finalGameUpdates) {
        outFile << key << ": " << val << "\n";
    }
    outFile << teamA << " stats:\n";
    for (auto const& [key, val] : finalTeamAUpdates) {
        outFile << key << ": " << val << "\n";
    }
    outFile << teamB << " stats:\n";
    for (auto const& [key, val] : finalTeamBUpdates) {
        outFile << key << ": " << val << "\n";
    }
    outFile << "Game event reports:\n";
    for (const auto& event : events) {
        outFile << event.get_time() << " - " << event.get_name() << ":\n\n";
        outFile << event.get_discription() << "\n\n"; 
    }
    outFile.close();
    std::cout << "Summary created successfully in " << fileName << std::endl;
}

void StompProtocol::handleLogout(ConnectionHandler& handler){
    std::unique_lock<std::mutex> lock(logoutMutex); 
    isLogoutProcessed = false;
    int receiptId = receiptIdCounter++; 
    this->expectedLogoutId = receiptId; 
    std::string frame = "DISCONNECT\n";
    frame += "receipt:" + std::to_string(receiptId) + "\n\n";  
    handler.sendFrameAscii(frame, '\0');
    std::cout << "Sending logout request (ID: " << receiptId << ")..." << std::endl;
    logoutCv.wait(lock, [this]{ return isLogoutProcessed; });
    shouldTerminate_ = true;
    isConnected_ = false;
    std::cout << "Logout Successful. Press Enter to exit." << std::endl;
} 

void StompProtocol::processServerFrame(std::string frame) {
    std::stringstream ss(frame);
    std::string command;
    std::getline(ss, command);
    if (command == "CONNECTED") {
        handleConnected(frame);
    } 
    else if (command == "MESSAGE") {
        handleMessage(frame);
    } 
    else if (command == "RECEIPT") {
        handleReceipt(frame);
    } 
    else if (command == "ERROR") {
        handleError(frame);
    }
}

void StompProtocol::handleConnected(std::string frame) {
    std::cout << "Login successful" << std::endl;
    isConnected_ = true;
    loginCv.notify_all();
}

void StompProtocol::handleMessage(std::string frame) {
    size_t bodyPos = frame.find("\n\n");
    std::string body = "";
    if (bodyPos != std::string::npos) {
        body = frame.substr(bodyPos + 2);
    }
    
    Event newEvent = parseEventBody(body); 
    
    std::string gameName = extractHeader(frame, "destination");
    std::string user = extractHeader(frame, "user"); 
    
    if (gameName.find("/topic/") == 0) gameName = gameName.substr(7);
    if (gameName.find("/") == 0) gameName = gameName.substr(1);

    if (gameName.empty()) {
        //maybe cooked, maybe not - probaly we are cooked..
        return;
    }
    
    {
        std::lock_guard<std::mutex> lock(protocolMutex);
        gameEvents[gameName][user].push_back(newEvent);
    }
    std::cout << "Update received from " << gameName << ": " << newEvent.get_name() << std::endl;
}

void StompProtocol::handleReceipt(std::string frame) {
    std::string idStr = extractHeader(frame, "receipt-id");    
    if (idStr.empty()) return; 
    int receivedId = std::stoi(idStr);
    
    if (receivedId == this->expectedLogoutId) {
        {
            std::lock_guard<std::mutex> lock(logoutMutex);
            isLogoutProcessed = true;
        }
        logoutCv.notify_all(); 
    }
    else {
        std::cout << "Receipt received: " << receivedId << std::endl;
    }
}

void StompProtocol::handleError(std::string frame) {
    std::cout << "Error received from server:" << std::endl;
    std::cout << frame << std::endl;
    isConnected_ = false;
    shouldTerminate_ = true;
    loginCv.notify_all();  
    logoutCv.notify_all();   
}