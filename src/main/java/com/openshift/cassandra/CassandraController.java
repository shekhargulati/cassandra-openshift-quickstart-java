package com.openshift.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class CassandraController {

	@RequestMapping(value = "/cassandra", method = RequestMethod.GET)
	public String process() throws TException, InvalidRequestException,
			UnavailableException, UnsupportedEncodingException,
			NotFoundException, TimedOutException {

		String host = System.getenv("OPENSHIFT_INTERNAL_IP");
		int port = 19160;
		TTransport transport = new TFramedTransport(new TSocket(host,port));
        TProtocol protocol = new TBinaryProtocol(transport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        client.set_keyspace("mykeyspace");

        // define column parent
        ColumnParent parent = new ColumnParent("User");

        // define row id
        ByteBuffer rowid = ByteBuffer.wrap("100".getBytes());

        // define column to add
        Column username = new Column();
        username.setName("username".getBytes());
        username.setValue("shekhargulati".getBytes());
        username.setTimestamp(System.currentTimeMillis());

        // define consistency level
        ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

        // execute insert
        client.insert(rowid, parent, username, consistencyLevel);
        
        Column password = new Column();
        password.setName("password".getBytes());
        password.setValue("password".getBytes());
        password.setTimestamp(System.currentTimeMillis());
        client.insert(rowid, parent, password, consistencyLevel);

        // release resources
        transport.flush();
        transport.close();
        return "hello";
	}
	
	
}
