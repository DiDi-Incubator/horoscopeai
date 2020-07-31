namespace java com.didichuxing.horoscope.core


const i32 ERRNO_OK = 0//成功
const i32 ERRNO_TIMEOUT = 1//请求超时
const i32 ERRNO_EXECUTE = 2//执行错误
const i32 ERRNO_CHECK = 3// 提交前校验错误
const i32 ERRNO_BACKPRESS = 4//反压
const i32 ERRNO_COMMIT = 5//提交错误
const i32 ERRNO_INVAL = 6//返回格式错误
const i32 ERRNO_UNKNOWN  = 999//未知错误 不可重试

struct ResponseHeader {
    1: required i32 errno;
    2: optional string msg;
    3: required string traceInfo;
}

struct EventRequest {
    1: required string traceInfo;
    2: required map<i32, binary> seqEvents;
}

struct EventReply {
    1: required ResponseHeader header;
    2: optional map<i32, string> seqEventIds;
}

struct SingleEventRequest {
    1: required string traceInfo;
    2: required binary event;
}

struct SingleEventReply {
    1: required ResponseHeader header;
    2: optional binary instance;
}

service EventBusService {
    EventReply putEvent(1:EventRequest request)
    SingleEventReply putEventSync(1:SingleEventRequest request)
}