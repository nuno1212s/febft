@0xeed7170ad58e7c02;

struct SystemMessage {
    union {
        request @0 :Void;
        consensus @1 :ConsensusMessage;
    }
}

struct ConsensusMessage {
    sequenceNumber @0 :Int32;
    messageKind @1 :ConsensusMessageKind;
}

struct ConsensusMessageKind {
    union {
        prePrepare @0 :Data;
        prepare @1 :Void;
        commit @2 :Void;
    }
}