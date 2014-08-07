#pragma once

#include "ofMain.h"

// For synchronizing multiple data streams that are not synchronized, by manually setting custom delay.
// For example, when combining Kinect depth, BlackMagic camera and other high frequency sensors,
// they are apparently not syncronized.
// Please inherit ReceiverImpl<T> with desired buffer type.

#define STREAM_SYNCHRONIZER_BEGIN_NAMESPACE namespace ofxStreamSynchronizer {
#define STREAM_SYNCHRONIZER_END_NAMESPACE }

STREAM_SYNCHRONIZER_BEGIN_NAMESPACE

template <typename T>
class DelayBuffer
{
    vector<T> buffer;
    vector<uint64_t> timestamp;
    int counter;
    
public:
    void setup(int sz, const T& val) {
        buffer.assign(sz, val);
        timestamp.assign(sz, 0);
        counter = 0;
    }
    
    void resize(int newsz) {
        buffer.resize(newsz);
        timestamp.assign(newsz, 0);
    }
    
    void enqueue(const T& queue, uint64_t ts) {
        buffer.at(counter % buffer.size()) = queue;
        timestamp.at(counter % timestamp.size()) = ts;
        counter++;
    }
    
    const T& getDelayed() const {
        return buffer.at((counter) % buffer.size());
    }

    const T& getNow() const {
        return buffer.at((counter - 1) % buffer.size());
    }

    const T& get(uint64_t ts) const {
        int c = counter;
        
        // delayed to now
        for (int i=c; i<c+buffer.size(); ++i) {
            if (timestamp.at(i % timestamp.size()) > ts) {
                if (i == c) {
                    return buffer.at((i) % buffer.size());
                } else {
                    return buffer.at((i - 1) % buffer.size());
                }
            }
        }
        return getDelayed();
    }

};


//-------------------------------------------------------------------------------------
class Service;

class Receiver
{
    friend class Service;
    
private:
    uint64_t ts; // handled by Service

protected:
    ofBuffer recordBuffer;
    inline uint64_t getTs() const {return ts;}

public:
    virtual void setupForLiveSource() {}
    virtual void setupForFile() {}

protected:
    Receiver() : ts(0) {}
    virtual ~Receiver() {}
    
    // return true if new frame arrived.
    virtual bool update(uint64_t ts) = 0;
    
    // fill recordBuffer with buffer for recording.
    virtual void createRecordBuffer() = 0;
    
    // decode buffer for playback.
    virtual void updateFromBuffer(uint64_t ts, const ofBuffer& buffer) = 0;
    
    // return unique id.
    virtual int getTypeId() = 0;
};

template <typename T>
class ReceiverImpl : public Receiver
{
protected:
    ReceiverImpl() : delayMillis(0) {}
    virtual ~ReceiverImpl() {}
    
    uint64_t delayMillis;
    DelayBuffer<T> delayBuffer;
    
public:
    void setDelayMillis(uint64_t delay, uint64_t expectedMessageIntervalMillis) {
        delayMillis = delay;
        int bufferSize = 2 + (delayMillis / expectedMessageIntervalMillis);
        delayBuffer.setup(bufferSize, T());
    }
    
    const T& get() const {
        if (delayMillis == 0) {
            return delayBuffer.getNow();
        }
        return delayBuffer.get(getTs() - delayMillis);
    }
};

//-------------------------------------------------------------------------------------
class Service : public ofThread
{
    struct RecordHeader {
        int typeId;
        int bodyLength;
        uint64_t timestamp;
        
        RecordHeader() : typeId(-1), bodyLength(0), timestamp(0) {}
        RecordHeader(int tid, int length, uint64_t ts)
        : typeId(tid), bodyLength(length), timestamp(ts) {}
    };
    
    vector<Receiver*> receivers;
    bool bRecording;
    bool bPlayback;
    ofFile fileRecord;
    
    // playback related
    ofFile filePlayback;
    uint64_t playbackStartTs;
    uint64_t recordedStartTs;
    RecordHeader currentHeader;

public:
    Service() {
        bRecording = false;
        bPlayback = false;
    }

    ~Service() {
    }
    
    bool isRecording() {return bRecording;}
    bool isPlayback() {return bPlayback;}
    
    void stop() {
        lock();
        receivers.clear();
        unlock();
        waitForThread(true);
    }
    
    // this method should be called before setup
    void registerReceiver(Receiver* receiver) {
        receivers.push_back(receiver);
    }
    
    // for live source
    void setup() {
        for (Receiver* r : receivers) {
            r->setupForLiveSource();
        }
        startThread();
    }
    
    // for playback
    void setup(string path) {
        for (Receiver* r : receivers) {
            r->setupForFile();
        }
        
        filePlayback.open(path, ofFile::ReadOnly, true);
        filePlayback.read(reinterpret_cast<char*>(&recordedStartTs), sizeof(uint64_t));
        playbackStartTs = ofGetElapsedTimeMillis();
        
        // read first frame header
        filePlayback.read(reinterpret_cast<char*>(&currentHeader), sizeof(currentHeader));
        
        bPlayback = true;
        startThread();
    }
    
    void startRecording(string path) {
        if (bPlayback) {
            return;
        }
        
        if (bRecording) {
            stopRecording();
        }
        
        recordFileLock.lock();
        fileRecord.open(path, ofFile::WriteOnly, true);
        
        // write current timestamp
        uint64_t ts = ofGetElapsedTimeMillis();
        ofBuffer tsBuffer;
        tsBuffer.append(reinterpret_cast<char*>(&ts), sizeof(uint64_t));
        fileRecord.writeFromBuffer(tsBuffer);
        recordFileLock.unlock();

        bRecording = true;
    }
    
    void stopRecording() {
        if (bPlayback) {
            return;
        }
        if (bRecording) {
            recordFileLock.lock();
            fileRecord.close();
            recordFileLock.unlock();
        }
        bRecording = false;
    }
    
protected:
    void threadedFunction() {
        while(isThreadRunning()) {
            uint64_t ts = ofGetElapsedTimeMillis();
            lock();
            if (bPlayback) {
                uint64_t playbackTs = ts - playbackStartTs + recordedStartTs;
                while (playbackTs >= currentHeader.timestamp) {
                    ofBuffer buff;
                    buff.allocate(currentHeader.bodyLength);
                    filePlayback.read(buff.getBinaryBuffer(), currentHeader.bodyLength);
                    for (Receiver* r : receivers) {
                        r->ts = playbackTs;
                        if (r->getTypeId() == currentHeader.typeId) {
                            r->updateFromBuffer(playbackTs, buff);
                            break;
                        }
                    }
                    
                    if (filePlayback.eof()) {
                        unlock();
                        return;
                    }
                    
                    filePlayback.read(reinterpret_cast<char*>(&currentHeader), sizeof(currentHeader));
                }
            } else {
                for (Receiver* r : receivers) {
                    r->ts = ts;
                    bool newMessage = r->update(ts);
                    if (newMessage && bRecording) {
                        recordFileLock.lock();
                        
                        // create body
                        r->createRecordBuffer();
                        
                        // write header
                        RecordHeader header(r->getTypeId(), r->recordBuffer.size(), ts);
                        ofBuffer tsBuffer;
                        tsBuffer.append(reinterpret_cast<char*>(&header), sizeof(RecordHeader));
                        tsBuffer.writeTo(fileRecord);
                        
                        // write body
                        r->recordBuffer.writeTo(fileRecord);
                        recordFileLock.unlock();
                    }
                }
            }
            unlock();
            sleep(1);
        }
    }
    
    Poco::FastMutex recordFileLock;
};


STREAM_SYNCHRONIZER_END_NAMESPACE