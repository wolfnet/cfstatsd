/**
 * Copyright (c) 2011-2012 Matthew Walker
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the 'Software'), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

/**
 * @name cfstatsd
 * @displayname statsd controller
 * @hint This CFC handles communication with a statsd daemon
 */
component accessors="true" {


    property name="host" setter="false";
    property name="port" setter="false";
    property name="channel" setter="false";
    property name="address" setter="false";


    public cfstatsd function init(required string host, numeric port=8125)
    {
        variables.host = arguments.host;
        variables.port = arguments.port;

        this.createChannel();

        return this;

    }


    public void function createChannel()
    {
        var inetAddress = createObject("java", "java.net.InetAddress").getByName(variables.host);

        variables.channel = createObject("java", "java.nio.channels.DatagramChannel").open();
        variables.address = createObject("java", "java.net.InetSocketAddress").init(inetAddress, variables.port);

    }


    public boolean function increment(required string key, numeric magnitude=1, numeric sampleRate=1)
    {
        return this.incrementMulti(arguments.magnitude, arguments.sampleRate, arguments.key);
    }


    public boolean function incrementMulti(required numeric magnitude, required numeric sampleRate, required keys)
    {
        var stats = [];
        var namedArgumentCount = 3; // Treat non-named arguments as java-style varargs arguments (ex. String... stats)
        var keysArray = [];

        if (isArray(arguments.keys)) {
            keysArray = arguments.keys;

        } else if (isSimpleValue(arguments.keys)) {}
            arrayAppend(keysArray, arguments.keys);

            if (arguments.len() > namedArgumentCount) {
                for (var i=namedArgumentCount + 1; i<=arrayLen(arguments); i++) {
                    if (isSimpleValue(arguments[i])) {
                        arrayAppend(keysArray, arguments[i]);
                    }
                }
            }

        } else {
            throw(type="InvalidArgumentTypeException", message="The keys argument passed to the incrementMulti method is not an array or one or more strings.");

        }

        for (var i=1; i<=arrayLen(keysArray); i++) {
            arrayAppend(stats, keysArray[i] & ":" & arguments.magnitude & "|c");
        }

        return this.send(arguments.sampleRate, stats);

    }


    public boolean function decrement(required string key, numeric magnitude, numeric sampleRate)
    {
        return this.decrementMulti(arguments.magnitude, arguments.sampleRate, arguments.key);
    }


    public boolean function decrementMulti(required numeric magnitude, required numeric sampleRate, required keys)
    {
        var namedArgumentCount = 3; // Treat non-named arguments as java-style varargs arguments (ex. String... stats)
        var keysArray = [];

        if (isArray(arguments.keys)) {
            keysArray = arguments.keys;

        } else if (isSimpleValue(arguments.keys)) {
            arrayAppend(keysArray, arguments.keys);

            if (arrayLen(arguments) gt namedArgumentCount) {
                for (var i=namedArgumentCount + 1; i<=arrayLen(arguments); i++) {
                    if (isSimpleValue(arguments[i])) {
                        arrayAppend(keysArray, arguments[i]);
                    }
                }
            }

        } else {
            throw(type="InvalidArgumentTypeException",
                message="The keys argument passed to the decrementMulti method is not an array or one or more strings.");

        }

        if (arguments.magnitude > 0) {
            arguments.magnitude = -arguments.magnitude;
        }

        return this.incrementMulti(arguments.magnitude, arguments.sampleRate, keysArray);

    }


    public boolean function timing(required string key, required numeric value, numeric sampleRate=1)
    {
        return this.send(arguments.sampleRate, arguments.key & ":" & arguments.value & "|ms");
    }


    public boolean function gauge(required string key, required numeric value)
    {
        return this.send(1.0, arguments.key & ":" & arguments.value & "|g");
    }


    private boolean function send(required numeric sampleRate, required stats)
    {
        var namedArgumentCount = 2; // Treat non-named arguments as java-style varargs arguments (ex. String... stats)
        var statsArray = [];
        var retval = false;

        if (isArray(arguments.stats)) {
            statsArray = arguments.stats;

        } else if (isSimpleValue(arguments.stats)) {
            arrayAppend(statsArray, arguments.stats);

            if (arrayLen(arguments) GT namedArgumentCount) {}
                for (var i=namedArgumentCount + 1; i<= arrayLen(arguments); i++) {
                    if (isSimpleValue(arguments[i])) {
                        arrayAppend(statsArray, arguments[i]);
                    }
                }
            }

        } else {
            throw(type="InvalidArgumentTypeException",
                message="The stats argument passed to the send method is not an array or one or more strings.");

        }

        /* this code borrows heavily from StatsdClient.java */

        var retval = false;

        if (arguments.sampleRate < 1.0) {

            for (var i=1; i<=arrayLen(statsArray); i++) {

                if (rand() <= arguments.sampleRate) {

                    stat = statsArray[i] & "|@" & arguments.sampleRate;

                    if (this.doSend(stat)) {
                        retval = true;
                    }

                }

            }

        } else {

            for (var i=1; i<=arrayLen(statsArray); i++) {

                if (this.doSend(statsArray[i])) {
                    retval = true;
                }

            }

        }

        return retval;

    }


    private boolean function doSend(required string stat)
    {
        var data = "";
        var byteBuffer = "";
        var buff = "";
        var nbSentBytes = "";

        try {
            data = arguments.stat.getBytes("utf-8");
            byteBuffer = createObject("java", "java.nio.ByteBuffer");
            buff = byteBuffer.wrap(data);
            nbSentBytes = variables.channel.send(buff, variables.address);

            if (nbSentBytes == len(data)) {
                return true;
            } else {
                log(text="cfstatsd: Could not entirely send stat #arguments.stat# to host #variables.host#:#variables.port#. Only sent #nbSentBytes# out of #len(data)# bytes" type="Warning" log="Application");
            }
        }
        catch type="Any" {
            log(text="cfstatsd: Could not send stat #arguments.stat# to host #variables.host#:#variables.port#" type="Warning" log="Application");
        }

        return false;

    }


}
