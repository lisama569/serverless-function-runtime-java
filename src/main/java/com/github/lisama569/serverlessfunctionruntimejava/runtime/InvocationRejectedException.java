package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import com.github.lisama569.serverlessfunctionruntimejava.model.RejectionReason;

/** 调用被拒绝时抛出，携带可区分的失败原因。 */
public class InvocationRejectedException extends RuntimeException {

    private final RejectionReason reason;

    public InvocationRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason getReason() {
        return reason;
    }
}
