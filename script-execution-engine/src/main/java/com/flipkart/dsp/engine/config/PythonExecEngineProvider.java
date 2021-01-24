package com.flipkart.dsp.engine.config;

import com.flipkart.dsp.engine.helper.PythonInputScriptGenerationHelper;
import com.flipkart.dsp.engine.engine.PyExecEngine;
import com.flipkart.dsp.engine.thrift.ScriptExecutionEngineImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
import javax.inject.Provider;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PythonExecEngineProvider implements Provider<PyExecEngine> {

    private final PythonInputScriptGenerationHelper pythonInputScriptGenerationHelper;

    @Override
    public PyExecEngine get() {
        try {
            return new PyExecEngine(providePyConnection(), pythonInputScriptGenerationHelper);
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    private ScriptExecutionEngineImpl.Client providePyConnection() throws TTransportException {
        TTransport transport = new TSocket("127.0.0.1", 9090);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new ScriptExecutionEngineImpl.Client(protocol);
    }
}
