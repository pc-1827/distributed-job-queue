class TokenBucket {
    constructor(maxTokens, refillRate) {
        this.maxTokens = maxTokens;
        this.refillRate = refillRate;
        this.tokens = maxTokens;

        setInterval(() => {
            this.tokens = Math.min(this.tokens + 1, this.maxTokens);
        }, refillRate);
    }

    consume() {
        if (this.tokens > 0) {
            this.tokens--;
            return true;
        }
        return false;
    }
}

module.exports = TokenBucket;
