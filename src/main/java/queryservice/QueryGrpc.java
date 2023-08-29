package queryservice;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Query defines the tablet query service, implemented by vttablet.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.42.1)",
    comments = "Source: queryservice.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class QueryGrpc {

  private QueryGrpc() {}

  public static final String SERVICE_NAME = "queryservice.Query";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ExecuteRequest,
      io.vitess.proto.Query.ExecuteResponse> getExecuteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Execute",
      requestType = io.vitess.proto.Query.ExecuteRequest.class,
      responseType = io.vitess.proto.Query.ExecuteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ExecuteRequest,
      io.vitess.proto.Query.ExecuteResponse> getExecuteMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ExecuteRequest, io.vitess.proto.Query.ExecuteResponse> getExecuteMethod;
    if ((getExecuteMethod = QueryGrpc.getExecuteMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getExecuteMethod = QueryGrpc.getExecuteMethod) == null) {
          QueryGrpc.getExecuteMethod = getExecuteMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ExecuteRequest, io.vitess.proto.Query.ExecuteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Execute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ExecuteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("Execute"))
              .build();
        }
      }
    }
    return getExecuteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ExecuteBatchRequest,
      io.vitess.proto.Query.ExecuteBatchResponse> getExecuteBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExecuteBatch",
      requestType = io.vitess.proto.Query.ExecuteBatchRequest.class,
      responseType = io.vitess.proto.Query.ExecuteBatchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ExecuteBatchRequest,
      io.vitess.proto.Query.ExecuteBatchResponse> getExecuteBatchMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ExecuteBatchRequest, io.vitess.proto.Query.ExecuteBatchResponse> getExecuteBatchMethod;
    if ((getExecuteBatchMethod = QueryGrpc.getExecuteBatchMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getExecuteBatchMethod = QueryGrpc.getExecuteBatchMethod) == null) {
          QueryGrpc.getExecuteBatchMethod = getExecuteBatchMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ExecuteBatchRequest, io.vitess.proto.Query.ExecuteBatchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExecuteBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ExecuteBatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ExecuteBatchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("ExecuteBatch"))
              .build();
        }
      }
    }
    return getExecuteBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.StreamExecuteRequest,
      io.vitess.proto.Query.StreamExecuteResponse> getStreamExecuteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamExecute",
      requestType = io.vitess.proto.Query.StreamExecuteRequest.class,
      responseType = io.vitess.proto.Query.StreamExecuteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.StreamExecuteRequest,
      io.vitess.proto.Query.StreamExecuteResponse> getStreamExecuteMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.StreamExecuteRequest, io.vitess.proto.Query.StreamExecuteResponse> getStreamExecuteMethod;
    if ((getStreamExecuteMethod = QueryGrpc.getStreamExecuteMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getStreamExecuteMethod = QueryGrpc.getStreamExecuteMethod) == null) {
          QueryGrpc.getStreamExecuteMethod = getStreamExecuteMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.StreamExecuteRequest, io.vitess.proto.Query.StreamExecuteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamExecute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.StreamExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.StreamExecuteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("StreamExecute"))
              .build();
        }
      }
    }
    return getStreamExecuteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginRequest,
      io.vitess.proto.Query.BeginResponse> getBeginMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Begin",
      requestType = io.vitess.proto.Query.BeginRequest.class,
      responseType = io.vitess.proto.Query.BeginResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginRequest,
      io.vitess.proto.Query.BeginResponse> getBeginMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginRequest, io.vitess.proto.Query.BeginResponse> getBeginMethod;
    if ((getBeginMethod = QueryGrpc.getBeginMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getBeginMethod = QueryGrpc.getBeginMethod) == null) {
          QueryGrpc.getBeginMethod = getBeginMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.BeginRequest, io.vitess.proto.Query.BeginResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Begin"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.BeginRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.BeginResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("Begin"))
              .build();
        }
      }
    }
    return getBeginMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.CommitRequest,
      io.vitess.proto.Query.CommitResponse> getCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Commit",
      requestType = io.vitess.proto.Query.CommitRequest.class,
      responseType = io.vitess.proto.Query.CommitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.CommitRequest,
      io.vitess.proto.Query.CommitResponse> getCommitMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.CommitRequest, io.vitess.proto.Query.CommitResponse> getCommitMethod;
    if ((getCommitMethod = QueryGrpc.getCommitMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getCommitMethod = QueryGrpc.getCommitMethod) == null) {
          QueryGrpc.getCommitMethod = getCommitMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.CommitRequest, io.vitess.proto.Query.CommitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.CommitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.CommitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("Commit"))
              .build();
        }
      }
    }
    return getCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.RollbackRequest,
      io.vitess.proto.Query.RollbackResponse> getRollbackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rollback",
      requestType = io.vitess.proto.Query.RollbackRequest.class,
      responseType = io.vitess.proto.Query.RollbackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.RollbackRequest,
      io.vitess.proto.Query.RollbackResponse> getRollbackMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.RollbackRequest, io.vitess.proto.Query.RollbackResponse> getRollbackMethod;
    if ((getRollbackMethod = QueryGrpc.getRollbackMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getRollbackMethod = QueryGrpc.getRollbackMethod) == null) {
          QueryGrpc.getRollbackMethod = getRollbackMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.RollbackRequest, io.vitess.proto.Query.RollbackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rollback"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.RollbackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.RollbackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("Rollback"))
              .build();
        }
      }
    }
    return getRollbackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.PrepareRequest,
      io.vitess.proto.Query.PrepareResponse> getPrepareMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Prepare",
      requestType = io.vitess.proto.Query.PrepareRequest.class,
      responseType = io.vitess.proto.Query.PrepareResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.PrepareRequest,
      io.vitess.proto.Query.PrepareResponse> getPrepareMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.PrepareRequest, io.vitess.proto.Query.PrepareResponse> getPrepareMethod;
    if ((getPrepareMethod = QueryGrpc.getPrepareMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getPrepareMethod = QueryGrpc.getPrepareMethod) == null) {
          QueryGrpc.getPrepareMethod = getPrepareMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.PrepareRequest, io.vitess.proto.Query.PrepareResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Prepare"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.PrepareRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.PrepareResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("Prepare"))
              .build();
        }
      }
    }
    return getPrepareMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.CommitPreparedRequest,
      io.vitess.proto.Query.CommitPreparedResponse> getCommitPreparedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitPrepared",
      requestType = io.vitess.proto.Query.CommitPreparedRequest.class,
      responseType = io.vitess.proto.Query.CommitPreparedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.CommitPreparedRequest,
      io.vitess.proto.Query.CommitPreparedResponse> getCommitPreparedMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.CommitPreparedRequest, io.vitess.proto.Query.CommitPreparedResponse> getCommitPreparedMethod;
    if ((getCommitPreparedMethod = QueryGrpc.getCommitPreparedMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getCommitPreparedMethod = QueryGrpc.getCommitPreparedMethod) == null) {
          QueryGrpc.getCommitPreparedMethod = getCommitPreparedMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.CommitPreparedRequest, io.vitess.proto.Query.CommitPreparedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CommitPrepared"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.CommitPreparedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.CommitPreparedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("CommitPrepared"))
              .build();
        }
      }
    }
    return getCommitPreparedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.RollbackPreparedRequest,
      io.vitess.proto.Query.RollbackPreparedResponse> getRollbackPreparedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RollbackPrepared",
      requestType = io.vitess.proto.Query.RollbackPreparedRequest.class,
      responseType = io.vitess.proto.Query.RollbackPreparedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.RollbackPreparedRequest,
      io.vitess.proto.Query.RollbackPreparedResponse> getRollbackPreparedMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.RollbackPreparedRequest, io.vitess.proto.Query.RollbackPreparedResponse> getRollbackPreparedMethod;
    if ((getRollbackPreparedMethod = QueryGrpc.getRollbackPreparedMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getRollbackPreparedMethod = QueryGrpc.getRollbackPreparedMethod) == null) {
          QueryGrpc.getRollbackPreparedMethod = getRollbackPreparedMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.RollbackPreparedRequest, io.vitess.proto.Query.RollbackPreparedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RollbackPrepared"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.RollbackPreparedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.RollbackPreparedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("RollbackPrepared"))
              .build();
        }
      }
    }
    return getRollbackPreparedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.CreateTransactionRequest,
      io.vitess.proto.Query.CreateTransactionResponse> getCreateTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTransaction",
      requestType = io.vitess.proto.Query.CreateTransactionRequest.class,
      responseType = io.vitess.proto.Query.CreateTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.CreateTransactionRequest,
      io.vitess.proto.Query.CreateTransactionResponse> getCreateTransactionMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.CreateTransactionRequest, io.vitess.proto.Query.CreateTransactionResponse> getCreateTransactionMethod;
    if ((getCreateTransactionMethod = QueryGrpc.getCreateTransactionMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getCreateTransactionMethod = QueryGrpc.getCreateTransactionMethod) == null) {
          QueryGrpc.getCreateTransactionMethod = getCreateTransactionMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.CreateTransactionRequest, io.vitess.proto.Query.CreateTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.CreateTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.CreateTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("CreateTransaction"))
              .build();
        }
      }
    }
    return getCreateTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.StartCommitRequest,
      io.vitess.proto.Query.StartCommitResponse> getStartCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartCommit",
      requestType = io.vitess.proto.Query.StartCommitRequest.class,
      responseType = io.vitess.proto.Query.StartCommitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.StartCommitRequest,
      io.vitess.proto.Query.StartCommitResponse> getStartCommitMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.StartCommitRequest, io.vitess.proto.Query.StartCommitResponse> getStartCommitMethod;
    if ((getStartCommitMethod = QueryGrpc.getStartCommitMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getStartCommitMethod = QueryGrpc.getStartCommitMethod) == null) {
          QueryGrpc.getStartCommitMethod = getStartCommitMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.StartCommitRequest, io.vitess.proto.Query.StartCommitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartCommit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.StartCommitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.StartCommitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("StartCommit"))
              .build();
        }
      }
    }
    return getStartCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.SetRollbackRequest,
      io.vitess.proto.Query.SetRollbackResponse> getSetRollbackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetRollback",
      requestType = io.vitess.proto.Query.SetRollbackRequest.class,
      responseType = io.vitess.proto.Query.SetRollbackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.SetRollbackRequest,
      io.vitess.proto.Query.SetRollbackResponse> getSetRollbackMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.SetRollbackRequest, io.vitess.proto.Query.SetRollbackResponse> getSetRollbackMethod;
    if ((getSetRollbackMethod = QueryGrpc.getSetRollbackMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getSetRollbackMethod = QueryGrpc.getSetRollbackMethod) == null) {
          QueryGrpc.getSetRollbackMethod = getSetRollbackMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.SetRollbackRequest, io.vitess.proto.Query.SetRollbackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetRollback"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.SetRollbackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.SetRollbackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("SetRollback"))
              .build();
        }
      }
    }
    return getSetRollbackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ConcludeTransactionRequest,
      io.vitess.proto.Query.ConcludeTransactionResponse> getConcludeTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ConcludeTransaction",
      requestType = io.vitess.proto.Query.ConcludeTransactionRequest.class,
      responseType = io.vitess.proto.Query.ConcludeTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ConcludeTransactionRequest,
      io.vitess.proto.Query.ConcludeTransactionResponse> getConcludeTransactionMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ConcludeTransactionRequest, io.vitess.proto.Query.ConcludeTransactionResponse> getConcludeTransactionMethod;
    if ((getConcludeTransactionMethod = QueryGrpc.getConcludeTransactionMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getConcludeTransactionMethod = QueryGrpc.getConcludeTransactionMethod) == null) {
          QueryGrpc.getConcludeTransactionMethod = getConcludeTransactionMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ConcludeTransactionRequest, io.vitess.proto.Query.ConcludeTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ConcludeTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ConcludeTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ConcludeTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("ConcludeTransaction"))
              .build();
        }
      }
    }
    return getConcludeTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ReadTransactionRequest,
      io.vitess.proto.Query.ReadTransactionResponse> getReadTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadTransaction",
      requestType = io.vitess.proto.Query.ReadTransactionRequest.class,
      responseType = io.vitess.proto.Query.ReadTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ReadTransactionRequest,
      io.vitess.proto.Query.ReadTransactionResponse> getReadTransactionMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ReadTransactionRequest, io.vitess.proto.Query.ReadTransactionResponse> getReadTransactionMethod;
    if ((getReadTransactionMethod = QueryGrpc.getReadTransactionMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getReadTransactionMethod = QueryGrpc.getReadTransactionMethod) == null) {
          QueryGrpc.getReadTransactionMethod = getReadTransactionMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ReadTransactionRequest, io.vitess.proto.Query.ReadTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReadTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReadTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("ReadTransaction"))
              .build();
        }
      }
    }
    return getReadTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginExecuteRequest,
      io.vitess.proto.Query.BeginExecuteResponse> getBeginExecuteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BeginExecute",
      requestType = io.vitess.proto.Query.BeginExecuteRequest.class,
      responseType = io.vitess.proto.Query.BeginExecuteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginExecuteRequest,
      io.vitess.proto.Query.BeginExecuteResponse> getBeginExecuteMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginExecuteRequest, io.vitess.proto.Query.BeginExecuteResponse> getBeginExecuteMethod;
    if ((getBeginExecuteMethod = QueryGrpc.getBeginExecuteMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getBeginExecuteMethod = QueryGrpc.getBeginExecuteMethod) == null) {
          QueryGrpc.getBeginExecuteMethod = getBeginExecuteMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.BeginExecuteRequest, io.vitess.proto.Query.BeginExecuteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BeginExecute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.BeginExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.BeginExecuteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("BeginExecute"))
              .build();
        }
      }
    }
    return getBeginExecuteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginExecuteBatchRequest,
      io.vitess.proto.Query.BeginExecuteBatchResponse> getBeginExecuteBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BeginExecuteBatch",
      requestType = io.vitess.proto.Query.BeginExecuteBatchRequest.class,
      responseType = io.vitess.proto.Query.BeginExecuteBatchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginExecuteBatchRequest,
      io.vitess.proto.Query.BeginExecuteBatchResponse> getBeginExecuteBatchMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.BeginExecuteBatchRequest, io.vitess.proto.Query.BeginExecuteBatchResponse> getBeginExecuteBatchMethod;
    if ((getBeginExecuteBatchMethod = QueryGrpc.getBeginExecuteBatchMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getBeginExecuteBatchMethod = QueryGrpc.getBeginExecuteBatchMethod) == null) {
          QueryGrpc.getBeginExecuteBatchMethod = getBeginExecuteBatchMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.BeginExecuteBatchRequest, io.vitess.proto.Query.BeginExecuteBatchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BeginExecuteBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.BeginExecuteBatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.BeginExecuteBatchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("BeginExecuteBatch"))
              .build();
        }
      }
    }
    return getBeginExecuteBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.MessageStreamRequest,
      io.vitess.proto.Query.MessageStreamResponse> getMessageStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MessageStream",
      requestType = io.vitess.proto.Query.MessageStreamRequest.class,
      responseType = io.vitess.proto.Query.MessageStreamResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.MessageStreamRequest,
      io.vitess.proto.Query.MessageStreamResponse> getMessageStreamMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.MessageStreamRequest, io.vitess.proto.Query.MessageStreamResponse> getMessageStreamMethod;
    if ((getMessageStreamMethod = QueryGrpc.getMessageStreamMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getMessageStreamMethod = QueryGrpc.getMessageStreamMethod) == null) {
          QueryGrpc.getMessageStreamMethod = getMessageStreamMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.MessageStreamRequest, io.vitess.proto.Query.MessageStreamResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MessageStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.MessageStreamRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.MessageStreamResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("MessageStream"))
              .build();
        }
      }
    }
    return getMessageStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.MessageAckRequest,
      io.vitess.proto.Query.MessageAckResponse> getMessageAckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MessageAck",
      requestType = io.vitess.proto.Query.MessageAckRequest.class,
      responseType = io.vitess.proto.Query.MessageAckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.MessageAckRequest,
      io.vitess.proto.Query.MessageAckResponse> getMessageAckMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.MessageAckRequest, io.vitess.proto.Query.MessageAckResponse> getMessageAckMethod;
    if ((getMessageAckMethod = QueryGrpc.getMessageAckMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getMessageAckMethod = QueryGrpc.getMessageAckMethod) == null) {
          QueryGrpc.getMessageAckMethod = getMessageAckMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.MessageAckRequest, io.vitess.proto.Query.MessageAckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MessageAck"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.MessageAckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.MessageAckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("MessageAck"))
              .build();
        }
      }
    }
    return getMessageAckMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ReserveExecuteRequest,
      io.vitess.proto.Query.ReserveExecuteResponse> getReserveExecuteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReserveExecute",
      requestType = io.vitess.proto.Query.ReserveExecuteRequest.class,
      responseType = io.vitess.proto.Query.ReserveExecuteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ReserveExecuteRequest,
      io.vitess.proto.Query.ReserveExecuteResponse> getReserveExecuteMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ReserveExecuteRequest, io.vitess.proto.Query.ReserveExecuteResponse> getReserveExecuteMethod;
    if ((getReserveExecuteMethod = QueryGrpc.getReserveExecuteMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getReserveExecuteMethod = QueryGrpc.getReserveExecuteMethod) == null) {
          QueryGrpc.getReserveExecuteMethod = getReserveExecuteMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ReserveExecuteRequest, io.vitess.proto.Query.ReserveExecuteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReserveExecute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReserveExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReserveExecuteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("ReserveExecute"))
              .build();
        }
      }
    }
    return getReserveExecuteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ReserveBeginExecuteRequest,
      io.vitess.proto.Query.ReserveBeginExecuteResponse> getReserveBeginExecuteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReserveBeginExecute",
      requestType = io.vitess.proto.Query.ReserveBeginExecuteRequest.class,
      responseType = io.vitess.proto.Query.ReserveBeginExecuteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ReserveBeginExecuteRequest,
      io.vitess.proto.Query.ReserveBeginExecuteResponse> getReserveBeginExecuteMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ReserveBeginExecuteRequest, io.vitess.proto.Query.ReserveBeginExecuteResponse> getReserveBeginExecuteMethod;
    if ((getReserveBeginExecuteMethod = QueryGrpc.getReserveBeginExecuteMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getReserveBeginExecuteMethod = QueryGrpc.getReserveBeginExecuteMethod) == null) {
          QueryGrpc.getReserveBeginExecuteMethod = getReserveBeginExecuteMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ReserveBeginExecuteRequest, io.vitess.proto.Query.ReserveBeginExecuteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReserveBeginExecute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReserveBeginExecuteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReserveBeginExecuteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("ReserveBeginExecute"))
              .build();
        }
      }
    }
    return getReserveBeginExecuteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.ReleaseRequest,
      io.vitess.proto.Query.ReleaseResponse> getReleaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Release",
      requestType = io.vitess.proto.Query.ReleaseRequest.class,
      responseType = io.vitess.proto.Query.ReleaseResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.ReleaseRequest,
      io.vitess.proto.Query.ReleaseResponse> getReleaseMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.ReleaseRequest, io.vitess.proto.Query.ReleaseResponse> getReleaseMethod;
    if ((getReleaseMethod = QueryGrpc.getReleaseMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getReleaseMethod = QueryGrpc.getReleaseMethod) == null) {
          QueryGrpc.getReleaseMethod = getReleaseMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.ReleaseRequest, io.vitess.proto.Query.ReleaseResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Release"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReleaseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.ReleaseResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("Release"))
              .build();
        }
      }
    }
    return getReleaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.vitess.proto.Query.StreamHealthRequest,
      io.vitess.proto.Query.StreamHealthResponse> getStreamHealthMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamHealth",
      requestType = io.vitess.proto.Query.StreamHealthRequest.class,
      responseType = io.vitess.proto.Query.StreamHealthResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.vitess.proto.Query.StreamHealthRequest,
      io.vitess.proto.Query.StreamHealthResponse> getStreamHealthMethod() {
    io.grpc.MethodDescriptor<io.vitess.proto.Query.StreamHealthRequest, io.vitess.proto.Query.StreamHealthResponse> getStreamHealthMethod;
    if ((getStreamHealthMethod = QueryGrpc.getStreamHealthMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getStreamHealthMethod = QueryGrpc.getStreamHealthMethod) == null) {
          QueryGrpc.getStreamHealthMethod = getStreamHealthMethod =
              io.grpc.MethodDescriptor.<io.vitess.proto.Query.StreamHealthRequest, io.vitess.proto.Query.StreamHealthResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamHealth"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.StreamHealthRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.vitess.proto.Query.StreamHealthResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("StreamHealth"))
              .build();
        }
      }
    }
    return getStreamHealthMethod;
  }

  private static volatile io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamRequest,
      binlogdata.Binlogdata.VStreamResponse> getVStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VStream",
      requestType = binlogdata.Binlogdata.VStreamRequest.class,
      responseType = binlogdata.Binlogdata.VStreamResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamRequest,
      binlogdata.Binlogdata.VStreamResponse> getVStreamMethod() {
    io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamRequest, binlogdata.Binlogdata.VStreamResponse> getVStreamMethod;
    if ((getVStreamMethod = QueryGrpc.getVStreamMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getVStreamMethod = QueryGrpc.getVStreamMethod) == null) {
          QueryGrpc.getVStreamMethod = getVStreamMethod =
              io.grpc.MethodDescriptor.<binlogdata.Binlogdata.VStreamRequest, binlogdata.Binlogdata.VStreamResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  binlogdata.Binlogdata.VStreamRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  binlogdata.Binlogdata.VStreamResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("VStream"))
              .build();
        }
      }
    }
    return getVStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamRowsRequest,
      binlogdata.Binlogdata.VStreamRowsResponse> getVStreamRowsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VStreamRows",
      requestType = binlogdata.Binlogdata.VStreamRowsRequest.class,
      responseType = binlogdata.Binlogdata.VStreamRowsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamRowsRequest,
      binlogdata.Binlogdata.VStreamRowsResponse> getVStreamRowsMethod() {
    io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamRowsRequest, binlogdata.Binlogdata.VStreamRowsResponse> getVStreamRowsMethod;
    if ((getVStreamRowsMethod = QueryGrpc.getVStreamRowsMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getVStreamRowsMethod = QueryGrpc.getVStreamRowsMethod) == null) {
          QueryGrpc.getVStreamRowsMethod = getVStreamRowsMethod =
              io.grpc.MethodDescriptor.<binlogdata.Binlogdata.VStreamRowsRequest, binlogdata.Binlogdata.VStreamRowsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VStreamRows"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  binlogdata.Binlogdata.VStreamRowsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  binlogdata.Binlogdata.VStreamRowsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("VStreamRows"))
              .build();
        }
      }
    }
    return getVStreamRowsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamResultsRequest,
      binlogdata.Binlogdata.VStreamResultsResponse> getVStreamResultsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VStreamResults",
      requestType = binlogdata.Binlogdata.VStreamResultsRequest.class,
      responseType = binlogdata.Binlogdata.VStreamResultsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamResultsRequest,
      binlogdata.Binlogdata.VStreamResultsResponse> getVStreamResultsMethod() {
    io.grpc.MethodDescriptor<binlogdata.Binlogdata.VStreamResultsRequest, binlogdata.Binlogdata.VStreamResultsResponse> getVStreamResultsMethod;
    if ((getVStreamResultsMethod = QueryGrpc.getVStreamResultsMethod) == null) {
      synchronized (QueryGrpc.class) {
        if ((getVStreamResultsMethod = QueryGrpc.getVStreamResultsMethod) == null) {
          QueryGrpc.getVStreamResultsMethod = getVStreamResultsMethod =
              io.grpc.MethodDescriptor.<binlogdata.Binlogdata.VStreamResultsRequest, binlogdata.Binlogdata.VStreamResultsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VStreamResults"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  binlogdata.Binlogdata.VStreamResultsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  binlogdata.Binlogdata.VStreamResultsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueryMethodDescriptorSupplier("VStreamResults"))
              .build();
        }
      }
    }
    return getVStreamResultsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static QueryStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueryStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueryStub>() {
        @Override
        public QueryStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueryStub(channel, callOptions);
        }
      };
    return QueryStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static QueryBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueryBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueryBlockingStub>() {
        @Override
        public QueryBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueryBlockingStub(channel, callOptions);
        }
      };
    return QueryBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static QueryFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueryFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueryFutureStub>() {
        @Override
        public QueryFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueryFutureStub(channel, callOptions);
        }
      };
    return QueryFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Query defines the tablet query service, implemented by vttablet.
   * </pre>
   */
  public static abstract class QueryImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Execute executes the specified SQL query (might be in a
     * transaction context, if Query.transaction_id is set).
     * </pre>
     */
    public void execute(io.vitess.proto.Query.ExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ExecuteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteMethod(), responseObserver);
    }

    /**
     * <pre>
     * ExecuteBatch executes a list of queries, and returns the result
     * for each query.
     * </pre>
     */
    public void executeBatch(io.vitess.proto.Query.ExecuteBatchRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ExecuteBatchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteBatchMethod(), responseObserver);
    }

    /**
     * <pre>
     * StreamExecute executes a streaming query. Use this method if the
     * query returns a large number of rows. The first QueryResult will
     * contain the Fields, subsequent QueryResult messages will contain
     * the rows.
     * </pre>
     */
    public void streamExecute(io.vitess.proto.Query.StreamExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamExecuteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStreamExecuteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Begin a transaction.
     * </pre>
     */
    public void begin(io.vitess.proto.Query.BeginRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBeginMethod(), responseObserver);
    }

    /**
     * <pre>
     * Commit a transaction.
     * </pre>
     */
    public void commit(io.vitess.proto.Query.CommitRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.CommitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
    }

    /**
     * <pre>
     * Rollback a transaction.
     * </pre>
     */
    public void rollback(io.vitess.proto.Query.RollbackRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.RollbackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRollbackMethod(), responseObserver);
    }

    /**
     * <pre>
     * Prepare preares a transaction.
     * </pre>
     */
    public void prepare(io.vitess.proto.Query.PrepareRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.PrepareResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrepareMethod(), responseObserver);
    }

    /**
     * <pre>
     * CommitPrepared commits a prepared transaction.
     * </pre>
     */
    public void commitPrepared(io.vitess.proto.Query.CommitPreparedRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.CommitPreparedResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitPreparedMethod(), responseObserver);
    }

    /**
     * <pre>
     * RollbackPrepared rolls back a prepared transaction.
     * </pre>
     */
    public void rollbackPrepared(io.vitess.proto.Query.RollbackPreparedRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.RollbackPreparedResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRollbackPreparedMethod(), responseObserver);
    }

    /**
     * <pre>
     * CreateTransaction creates the metadata for a 2pc transaction.
     * </pre>
     */
    public void createTransaction(io.vitess.proto.Query.CreateTransactionRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.CreateTransactionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTransactionMethod(), responseObserver);
    }

    /**
     * <pre>
     * StartCommit initiates a commit for a 2pc transaction.
     * </pre>
     */
    public void startCommit(io.vitess.proto.Query.StartCommitRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.StartCommitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartCommitMethod(), responseObserver);
    }

    /**
     * <pre>
     * SetRollback marks the 2pc transaction for rollback.
     * </pre>
     */
    public void setRollback(io.vitess.proto.Query.SetRollbackRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.SetRollbackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetRollbackMethod(), responseObserver);
    }

    /**
     * <pre>
     * ConcludeTransaction marks the 2pc transaction as resolved.
     * </pre>
     */
    public void concludeTransaction(io.vitess.proto.Query.ConcludeTransactionRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ConcludeTransactionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getConcludeTransactionMethod(), responseObserver);
    }

    /**
     * <pre>
     * ReadTransaction returns the 2pc transaction info.
     * </pre>
     */
    public void readTransaction(io.vitess.proto.Query.ReadTransactionRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReadTransactionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadTransactionMethod(), responseObserver);
    }

    /**
     * <pre>
     * BeginExecute executes a begin and the specified SQL query.
     * </pre>
     */
    public void beginExecute(io.vitess.proto.Query.BeginExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginExecuteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBeginExecuteMethod(), responseObserver);
    }

    /**
     * <pre>
     * BeginExecuteBatch executes a begin and a list of queries.
     * </pre>
     */
    public void beginExecuteBatch(io.vitess.proto.Query.BeginExecuteBatchRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginExecuteBatchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBeginExecuteBatchMethod(), responseObserver);
    }

    /**
     * <pre>
     * MessageStream streams messages from a message table.
     * </pre>
     */
    public void messageStream(io.vitess.proto.Query.MessageStreamRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.MessageStreamResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMessageStreamMethod(), responseObserver);
    }

    /**
     * <pre>
     * MessageAck acks messages for a table.
     * </pre>
     */
    public void messageAck(io.vitess.proto.Query.MessageAckRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.MessageAckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMessageAckMethod(), responseObserver);
    }

    /**
     */
    public void reserveExecute(io.vitess.proto.Query.ReserveExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReserveExecuteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReserveExecuteMethod(), responseObserver);
    }

    /**
     */
    public void reserveBeginExecute(io.vitess.proto.Query.ReserveBeginExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReserveBeginExecuteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReserveBeginExecuteMethod(), responseObserver);
    }

    /**
     */
    public void release(io.vitess.proto.Query.ReleaseRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReleaseResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReleaseMethod(), responseObserver);
    }

    /**
     * <pre>
     * StreamHealth runs a streaming RPC to the tablet, that returns the
     * current health of the tablet on a regular basis.
     * </pre>
     */
    public void streamHealth(io.vitess.proto.Query.StreamHealthRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamHealthResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStreamHealthMethod(), responseObserver);
    }

    /**
     * <pre>
     * VStream streams vreplication events.
     * </pre>
     */
    public void vStream(binlogdata.Binlogdata.VStreamRequest request,
        io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVStreamMethod(), responseObserver);
    }

    /**
     * <pre>
     * VStreamRows streams rows from the specified starting point.
     * </pre>
     */
    public void vStreamRows(binlogdata.Binlogdata.VStreamRowsRequest request,
        io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamRowsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVStreamRowsMethod(), responseObserver);
    }

    /**
     * <pre>
     * VStreamResults streams results along with the gtid of the snapshot.
     * </pre>
     */
    public void vStreamResults(binlogdata.Binlogdata.VStreamResultsRequest request,
        io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamResultsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVStreamResultsMethod(), responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getExecuteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ExecuteRequest,
                io.vitess.proto.Query.ExecuteResponse>(
                  this, METHODID_EXECUTE)))
          .addMethod(
            getExecuteBatchMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ExecuteBatchRequest,
                io.vitess.proto.Query.ExecuteBatchResponse>(
                  this, METHODID_EXECUTE_BATCH)))
          .addMethod(
            getStreamExecuteMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.vitess.proto.Query.StreamExecuteRequest,
                io.vitess.proto.Query.StreamExecuteResponse>(
                  this, METHODID_STREAM_EXECUTE)))
          .addMethod(
            getBeginMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.BeginRequest,
                io.vitess.proto.Query.BeginResponse>(
                  this, METHODID_BEGIN)))
          .addMethod(
            getCommitMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.CommitRequest,
                io.vitess.proto.Query.CommitResponse>(
                  this, METHODID_COMMIT)))
          .addMethod(
            getRollbackMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.RollbackRequest,
                io.vitess.proto.Query.RollbackResponse>(
                  this, METHODID_ROLLBACK)))
          .addMethod(
            getPrepareMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.PrepareRequest,
                io.vitess.proto.Query.PrepareResponse>(
                  this, METHODID_PREPARE)))
          .addMethod(
            getCommitPreparedMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.CommitPreparedRequest,
                io.vitess.proto.Query.CommitPreparedResponse>(
                  this, METHODID_COMMIT_PREPARED)))
          .addMethod(
            getRollbackPreparedMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.RollbackPreparedRequest,
                io.vitess.proto.Query.RollbackPreparedResponse>(
                  this, METHODID_ROLLBACK_PREPARED)))
          .addMethod(
            getCreateTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.CreateTransactionRequest,
                io.vitess.proto.Query.CreateTransactionResponse>(
                  this, METHODID_CREATE_TRANSACTION)))
          .addMethod(
            getStartCommitMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.StartCommitRequest,
                io.vitess.proto.Query.StartCommitResponse>(
                  this, METHODID_START_COMMIT)))
          .addMethod(
            getSetRollbackMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.SetRollbackRequest,
                io.vitess.proto.Query.SetRollbackResponse>(
                  this, METHODID_SET_ROLLBACK)))
          .addMethod(
            getConcludeTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ConcludeTransactionRequest,
                io.vitess.proto.Query.ConcludeTransactionResponse>(
                  this, METHODID_CONCLUDE_TRANSACTION)))
          .addMethod(
            getReadTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ReadTransactionRequest,
                io.vitess.proto.Query.ReadTransactionResponse>(
                  this, METHODID_READ_TRANSACTION)))
          .addMethod(
            getBeginExecuteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.BeginExecuteRequest,
                io.vitess.proto.Query.BeginExecuteResponse>(
                  this, METHODID_BEGIN_EXECUTE)))
          .addMethod(
            getBeginExecuteBatchMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.BeginExecuteBatchRequest,
                io.vitess.proto.Query.BeginExecuteBatchResponse>(
                  this, METHODID_BEGIN_EXECUTE_BATCH)))
          .addMethod(
            getMessageStreamMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.vitess.proto.Query.MessageStreamRequest,
                io.vitess.proto.Query.MessageStreamResponse>(
                  this, METHODID_MESSAGE_STREAM)))
          .addMethod(
            getMessageAckMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.MessageAckRequest,
                io.vitess.proto.Query.MessageAckResponse>(
                  this, METHODID_MESSAGE_ACK)))
          .addMethod(
            getReserveExecuteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ReserveExecuteRequest,
                io.vitess.proto.Query.ReserveExecuteResponse>(
                  this, METHODID_RESERVE_EXECUTE)))
          .addMethod(
            getReserveBeginExecuteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ReserveBeginExecuteRequest,
                io.vitess.proto.Query.ReserveBeginExecuteResponse>(
                  this, METHODID_RESERVE_BEGIN_EXECUTE)))
          .addMethod(
            getReleaseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.vitess.proto.Query.ReleaseRequest,
                io.vitess.proto.Query.ReleaseResponse>(
                  this, METHODID_RELEASE)))
          .addMethod(
            getStreamHealthMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.vitess.proto.Query.StreamHealthRequest,
                io.vitess.proto.Query.StreamHealthResponse>(
                  this, METHODID_STREAM_HEALTH)))
          .addMethod(
            getVStreamMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                binlogdata.Binlogdata.VStreamRequest,
                binlogdata.Binlogdata.VStreamResponse>(
                  this, METHODID_VSTREAM)))
          .addMethod(
            getVStreamRowsMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                binlogdata.Binlogdata.VStreamRowsRequest,
                binlogdata.Binlogdata.VStreamRowsResponse>(
                  this, METHODID_VSTREAM_ROWS)))
          .addMethod(
            getVStreamResultsMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                binlogdata.Binlogdata.VStreamResultsRequest,
                binlogdata.Binlogdata.VStreamResultsResponse>(
                  this, METHODID_VSTREAM_RESULTS)))
          .build();
    }
  }

  /**
   * <pre>
   * Query defines the tablet query service, implemented by vttablet.
   * </pre>
   */
  public static final class QueryStub extends io.grpc.stub.AbstractAsyncStub<QueryStub> {
    private QueryStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected QueryStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueryStub(channel, callOptions);
    }

    /**
     * <pre>
     * Execute executes the specified SQL query (might be in a
     * transaction context, if Query.transaction_id is set).
     * </pre>
     */
    public void execute(io.vitess.proto.Query.ExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ExecuteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExecuteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ExecuteBatch executes a list of queries, and returns the result
     * for each query.
     * </pre>
     */
    public void executeBatch(io.vitess.proto.Query.ExecuteBatchRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ExecuteBatchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExecuteBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * StreamExecute executes a streaming query. Use this method if the
     * query returns a large number of rows. The first QueryResult will
     * contain the Fields, subsequent QueryResult messages will contain
     * the rows.
     * </pre>
     */
    public void streamExecute(io.vitess.proto.Query.StreamExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamExecuteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getStreamExecuteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Begin a transaction.
     * </pre>
     */
    public void begin(io.vitess.proto.Query.BeginRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBeginMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Commit a transaction.
     * </pre>
     */
    public void commit(io.vitess.proto.Query.CommitRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.CommitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Rollback a transaction.
     * </pre>
     */
    public void rollback(io.vitess.proto.Query.RollbackRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.RollbackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRollbackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Prepare preares a transaction.
     * </pre>
     */
    public void prepare(io.vitess.proto.Query.PrepareRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.PrepareResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrepareMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * CommitPrepared commits a prepared transaction.
     * </pre>
     */
    public void commitPrepared(io.vitess.proto.Query.CommitPreparedRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.CommitPreparedResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitPreparedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * RollbackPrepared rolls back a prepared transaction.
     * </pre>
     */
    public void rollbackPrepared(io.vitess.proto.Query.RollbackPreparedRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.RollbackPreparedResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRollbackPreparedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * CreateTransaction creates the metadata for a 2pc transaction.
     * </pre>
     */
    public void createTransaction(io.vitess.proto.Query.CreateTransactionRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.CreateTransactionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * StartCommit initiates a commit for a 2pc transaction.
     * </pre>
     */
    public void startCommit(io.vitess.proto.Query.StartCommitRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.StartCommitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * SetRollback marks the 2pc transaction for rollback.
     * </pre>
     */
    public void setRollback(io.vitess.proto.Query.SetRollbackRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.SetRollbackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetRollbackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ConcludeTransaction marks the 2pc transaction as resolved.
     * </pre>
     */
    public void concludeTransaction(io.vitess.proto.Query.ConcludeTransactionRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ConcludeTransactionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getConcludeTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ReadTransaction returns the 2pc transaction info.
     * </pre>
     */
    public void readTransaction(io.vitess.proto.Query.ReadTransactionRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReadTransactionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * BeginExecute executes a begin and the specified SQL query.
     * </pre>
     */
    public void beginExecute(io.vitess.proto.Query.BeginExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginExecuteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBeginExecuteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * BeginExecuteBatch executes a begin and a list of queries.
     * </pre>
     */
    public void beginExecuteBatch(io.vitess.proto.Query.BeginExecuteBatchRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginExecuteBatchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBeginExecuteBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * MessageStream streams messages from a message table.
     * </pre>
     */
    public void messageStream(io.vitess.proto.Query.MessageStreamRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.MessageStreamResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getMessageStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * MessageAck acks messages for a table.
     * </pre>
     */
    public void messageAck(io.vitess.proto.Query.MessageAckRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.MessageAckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMessageAckMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reserveExecute(io.vitess.proto.Query.ReserveExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReserveExecuteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReserveExecuteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reserveBeginExecute(io.vitess.proto.Query.ReserveBeginExecuteRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReserveBeginExecuteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReserveBeginExecuteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void release(io.vitess.proto.Query.ReleaseRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReleaseResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * StreamHealth runs a streaming RPC to the tablet, that returns the
     * current health of the tablet on a regular basis.
     * </pre>
     */
    public void streamHealth(io.vitess.proto.Query.StreamHealthRequest request,
        io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamHealthResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getStreamHealthMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * VStream streams vreplication events.
     * </pre>
     */
    public void vStream(binlogdata.Binlogdata.VStreamRequest request,
        io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getVStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * VStreamRows streams rows from the specified starting point.
     * </pre>
     */
    public void vStreamRows(binlogdata.Binlogdata.VStreamRowsRequest request,
        io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamRowsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getVStreamRowsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * VStreamResults streams results along with the gtid of the snapshot.
     * </pre>
     */
    public void vStreamResults(binlogdata.Binlogdata.VStreamResultsRequest request,
        io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamResultsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getVStreamResultsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Query defines the tablet query service, implemented by vttablet.
   * </pre>
   */
  public static final class QueryBlockingStub extends io.grpc.stub.AbstractBlockingStub<QueryBlockingStub> {
    private QueryBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected QueryBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueryBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Execute executes the specified SQL query (might be in a
     * transaction context, if Query.transaction_id is set).
     * </pre>
     */
    public io.vitess.proto.Query.ExecuteResponse execute(io.vitess.proto.Query.ExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExecuteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ExecuteBatch executes a list of queries, and returns the result
     * for each query.
     * </pre>
     */
    public io.vitess.proto.Query.ExecuteBatchResponse executeBatch(io.vitess.proto.Query.ExecuteBatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExecuteBatchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * StreamExecute executes a streaming query. Use this method if the
     * query returns a large number of rows. The first QueryResult will
     * contain the Fields, subsequent QueryResult messages will contain
     * the rows.
     * </pre>
     */
    public java.util.Iterator<io.vitess.proto.Query.StreamExecuteResponse> streamExecute(
        io.vitess.proto.Query.StreamExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getStreamExecuteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Begin a transaction.
     * </pre>
     */
    public io.vitess.proto.Query.BeginResponse begin(io.vitess.proto.Query.BeginRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBeginMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Commit a transaction.
     * </pre>
     */
    public io.vitess.proto.Query.CommitResponse commit(io.vitess.proto.Query.CommitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Rollback a transaction.
     * </pre>
     */
    public io.vitess.proto.Query.RollbackResponse rollback(io.vitess.proto.Query.RollbackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRollbackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Prepare preares a transaction.
     * </pre>
     */
    public io.vitess.proto.Query.PrepareResponse prepare(io.vitess.proto.Query.PrepareRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrepareMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * CommitPrepared commits a prepared transaction.
     * </pre>
     */
    public io.vitess.proto.Query.CommitPreparedResponse commitPrepared(io.vitess.proto.Query.CommitPreparedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitPreparedMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * RollbackPrepared rolls back a prepared transaction.
     * </pre>
     */
    public io.vitess.proto.Query.RollbackPreparedResponse rollbackPrepared(io.vitess.proto.Query.RollbackPreparedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRollbackPreparedMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * CreateTransaction creates the metadata for a 2pc transaction.
     * </pre>
     */
    public io.vitess.proto.Query.CreateTransactionResponse createTransaction(io.vitess.proto.Query.CreateTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTransactionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * StartCommit initiates a commit for a 2pc transaction.
     * </pre>
     */
    public io.vitess.proto.Query.StartCommitResponse startCommit(io.vitess.proto.Query.StartCommitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartCommitMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * SetRollback marks the 2pc transaction for rollback.
     * </pre>
     */
    public io.vitess.proto.Query.SetRollbackResponse setRollback(io.vitess.proto.Query.SetRollbackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetRollbackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ConcludeTransaction marks the 2pc transaction as resolved.
     * </pre>
     */
    public io.vitess.proto.Query.ConcludeTransactionResponse concludeTransaction(io.vitess.proto.Query.ConcludeTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getConcludeTransactionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ReadTransaction returns the 2pc transaction info.
     * </pre>
     */
    public io.vitess.proto.Query.ReadTransactionResponse readTransaction(io.vitess.proto.Query.ReadTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadTransactionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * BeginExecute executes a begin and the specified SQL query.
     * </pre>
     */
    public io.vitess.proto.Query.BeginExecuteResponse beginExecute(io.vitess.proto.Query.BeginExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBeginExecuteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * BeginExecuteBatch executes a begin and a list of queries.
     * </pre>
     */
    public io.vitess.proto.Query.BeginExecuteBatchResponse beginExecuteBatch(io.vitess.proto.Query.BeginExecuteBatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBeginExecuteBatchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * MessageStream streams messages from a message table.
     * </pre>
     */
    public java.util.Iterator<io.vitess.proto.Query.MessageStreamResponse> messageStream(
        io.vitess.proto.Query.MessageStreamRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getMessageStreamMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * MessageAck acks messages for a table.
     * </pre>
     */
    public io.vitess.proto.Query.MessageAckResponse messageAck(io.vitess.proto.Query.MessageAckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMessageAckMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.vitess.proto.Query.ReserveExecuteResponse reserveExecute(io.vitess.proto.Query.ReserveExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReserveExecuteMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.vitess.proto.Query.ReserveBeginExecuteResponse reserveBeginExecute(io.vitess.proto.Query.ReserveBeginExecuteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReserveBeginExecuteMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.vitess.proto.Query.ReleaseResponse release(io.vitess.proto.Query.ReleaseRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReleaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * StreamHealth runs a streaming RPC to the tablet, that returns the
     * current health of the tablet on a regular basis.
     * </pre>
     */
    public java.util.Iterator<io.vitess.proto.Query.StreamHealthResponse> streamHealth(
        io.vitess.proto.Query.StreamHealthRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getStreamHealthMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * VStream streams vreplication events.
     * </pre>
     */
    public java.util.Iterator<binlogdata.Binlogdata.VStreamResponse> vStream(
        binlogdata.Binlogdata.VStreamRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getVStreamMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * VStreamRows streams rows from the specified starting point.
     * </pre>
     */
    public java.util.Iterator<binlogdata.Binlogdata.VStreamRowsResponse> vStreamRows(
        binlogdata.Binlogdata.VStreamRowsRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getVStreamRowsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * VStreamResults streams results along with the gtid of the snapshot.
     * </pre>
     */
    public java.util.Iterator<binlogdata.Binlogdata.VStreamResultsResponse> vStreamResults(
        binlogdata.Binlogdata.VStreamResultsRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getVStreamResultsMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Query defines the tablet query service, implemented by vttablet.
   * </pre>
   */
  public static final class QueryFutureStub extends io.grpc.stub.AbstractFutureStub<QueryFutureStub> {
    private QueryFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected QueryFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueryFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Execute executes the specified SQL query (might be in a
     * transaction context, if Query.transaction_id is set).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ExecuteResponse> execute(
        io.vitess.proto.Query.ExecuteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExecuteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ExecuteBatch executes a list of queries, and returns the result
     * for each query.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ExecuteBatchResponse> executeBatch(
        io.vitess.proto.Query.ExecuteBatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExecuteBatchMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Begin a transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.BeginResponse> begin(
        io.vitess.proto.Query.BeginRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBeginMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Commit a transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.CommitResponse> commit(
        io.vitess.proto.Query.CommitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Rollback a transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.RollbackResponse> rollback(
        io.vitess.proto.Query.RollbackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRollbackMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Prepare preares a transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.PrepareResponse> prepare(
        io.vitess.proto.Query.PrepareRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrepareMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * CommitPrepared commits a prepared transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.CommitPreparedResponse> commitPrepared(
        io.vitess.proto.Query.CommitPreparedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitPreparedMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * RollbackPrepared rolls back a prepared transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.RollbackPreparedResponse> rollbackPrepared(
        io.vitess.proto.Query.RollbackPreparedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRollbackPreparedMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * CreateTransaction creates the metadata for a 2pc transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.CreateTransactionResponse> createTransaction(
        io.vitess.proto.Query.CreateTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTransactionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * StartCommit initiates a commit for a 2pc transaction.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.StartCommitResponse> startCommit(
        io.vitess.proto.Query.StartCommitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartCommitMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * SetRollback marks the 2pc transaction for rollback.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.SetRollbackResponse> setRollback(
        io.vitess.proto.Query.SetRollbackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetRollbackMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ConcludeTransaction marks the 2pc transaction as resolved.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ConcludeTransactionResponse> concludeTransaction(
        io.vitess.proto.Query.ConcludeTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getConcludeTransactionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ReadTransaction returns the 2pc transaction info.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ReadTransactionResponse> readTransaction(
        io.vitess.proto.Query.ReadTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadTransactionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * BeginExecute executes a begin and the specified SQL query.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.BeginExecuteResponse> beginExecute(
        io.vitess.proto.Query.BeginExecuteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBeginExecuteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * BeginExecuteBatch executes a begin and a list of queries.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.BeginExecuteBatchResponse> beginExecuteBatch(
        io.vitess.proto.Query.BeginExecuteBatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBeginExecuteBatchMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * MessageAck acks messages for a table.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.MessageAckResponse> messageAck(
        io.vitess.proto.Query.MessageAckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMessageAckMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ReserveExecuteResponse> reserveExecute(
        io.vitess.proto.Query.ReserveExecuteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReserveExecuteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ReserveBeginExecuteResponse> reserveBeginExecute(
        io.vitess.proto.Query.ReserveBeginExecuteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReserveBeginExecuteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.vitess.proto.Query.ReleaseResponse> release(
        io.vitess.proto.Query.ReleaseRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_EXECUTE = 0;
  private static final int METHODID_EXECUTE_BATCH = 1;
  private static final int METHODID_STREAM_EXECUTE = 2;
  private static final int METHODID_BEGIN = 3;
  private static final int METHODID_COMMIT = 4;
  private static final int METHODID_ROLLBACK = 5;
  private static final int METHODID_PREPARE = 6;
  private static final int METHODID_COMMIT_PREPARED = 7;
  private static final int METHODID_ROLLBACK_PREPARED = 8;
  private static final int METHODID_CREATE_TRANSACTION = 9;
  private static final int METHODID_START_COMMIT = 10;
  private static final int METHODID_SET_ROLLBACK = 11;
  private static final int METHODID_CONCLUDE_TRANSACTION = 12;
  private static final int METHODID_READ_TRANSACTION = 13;
  private static final int METHODID_BEGIN_EXECUTE = 14;
  private static final int METHODID_BEGIN_EXECUTE_BATCH = 15;
  private static final int METHODID_MESSAGE_STREAM = 16;
  private static final int METHODID_MESSAGE_ACK = 17;
  private static final int METHODID_RESERVE_EXECUTE = 18;
  private static final int METHODID_RESERVE_BEGIN_EXECUTE = 19;
  private static final int METHODID_RELEASE = 20;
  private static final int METHODID_STREAM_HEALTH = 21;
  private static final int METHODID_VSTREAM = 22;
  private static final int METHODID_VSTREAM_ROWS = 23;
  private static final int METHODID_VSTREAM_RESULTS = 24;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final QueryImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(QueryImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_EXECUTE:
          serviceImpl.execute((io.vitess.proto.Query.ExecuteRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ExecuteResponse>) responseObserver);
          break;
        case METHODID_EXECUTE_BATCH:
          serviceImpl.executeBatch((io.vitess.proto.Query.ExecuteBatchRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ExecuteBatchResponse>) responseObserver);
          break;
        case METHODID_STREAM_EXECUTE:
          serviceImpl.streamExecute((io.vitess.proto.Query.StreamExecuteRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamExecuteResponse>) responseObserver);
          break;
        case METHODID_BEGIN:
          serviceImpl.begin((io.vitess.proto.Query.BeginRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginResponse>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((io.vitess.proto.Query.CommitRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.CommitResponse>) responseObserver);
          break;
        case METHODID_ROLLBACK:
          serviceImpl.rollback((io.vitess.proto.Query.RollbackRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.RollbackResponse>) responseObserver);
          break;
        case METHODID_PREPARE:
          serviceImpl.prepare((io.vitess.proto.Query.PrepareRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.PrepareResponse>) responseObserver);
          break;
        case METHODID_COMMIT_PREPARED:
          serviceImpl.commitPrepared((io.vitess.proto.Query.CommitPreparedRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.CommitPreparedResponse>) responseObserver);
          break;
        case METHODID_ROLLBACK_PREPARED:
          serviceImpl.rollbackPrepared((io.vitess.proto.Query.RollbackPreparedRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.RollbackPreparedResponse>) responseObserver);
          break;
        case METHODID_CREATE_TRANSACTION:
          serviceImpl.createTransaction((io.vitess.proto.Query.CreateTransactionRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.CreateTransactionResponse>) responseObserver);
          break;
        case METHODID_START_COMMIT:
          serviceImpl.startCommit((io.vitess.proto.Query.StartCommitRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.StartCommitResponse>) responseObserver);
          break;
        case METHODID_SET_ROLLBACK:
          serviceImpl.setRollback((io.vitess.proto.Query.SetRollbackRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.SetRollbackResponse>) responseObserver);
          break;
        case METHODID_CONCLUDE_TRANSACTION:
          serviceImpl.concludeTransaction((io.vitess.proto.Query.ConcludeTransactionRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ConcludeTransactionResponse>) responseObserver);
          break;
        case METHODID_READ_TRANSACTION:
          serviceImpl.readTransaction((io.vitess.proto.Query.ReadTransactionRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReadTransactionResponse>) responseObserver);
          break;
        case METHODID_BEGIN_EXECUTE:
          serviceImpl.beginExecute((io.vitess.proto.Query.BeginExecuteRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginExecuteResponse>) responseObserver);
          break;
        case METHODID_BEGIN_EXECUTE_BATCH:
          serviceImpl.beginExecuteBatch((io.vitess.proto.Query.BeginExecuteBatchRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.BeginExecuteBatchResponse>) responseObserver);
          break;
        case METHODID_MESSAGE_STREAM:
          serviceImpl.messageStream((io.vitess.proto.Query.MessageStreamRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.MessageStreamResponse>) responseObserver);
          break;
        case METHODID_MESSAGE_ACK:
          serviceImpl.messageAck((io.vitess.proto.Query.MessageAckRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.MessageAckResponse>) responseObserver);
          break;
        case METHODID_RESERVE_EXECUTE:
          serviceImpl.reserveExecute((io.vitess.proto.Query.ReserveExecuteRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReserveExecuteResponse>) responseObserver);
          break;
        case METHODID_RESERVE_BEGIN_EXECUTE:
          serviceImpl.reserveBeginExecute((io.vitess.proto.Query.ReserveBeginExecuteRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReserveBeginExecuteResponse>) responseObserver);
          break;
        case METHODID_RELEASE:
          serviceImpl.release((io.vitess.proto.Query.ReleaseRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.ReleaseResponse>) responseObserver);
          break;
        case METHODID_STREAM_HEALTH:
          serviceImpl.streamHealth((io.vitess.proto.Query.StreamHealthRequest) request,
              (io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamHealthResponse>) responseObserver);
          break;
        case METHODID_VSTREAM:
          serviceImpl.vStream((binlogdata.Binlogdata.VStreamRequest) request,
              (io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamResponse>) responseObserver);
          break;
        case METHODID_VSTREAM_ROWS:
          serviceImpl.vStreamRows((binlogdata.Binlogdata.VStreamRowsRequest) request,
              (io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamRowsResponse>) responseObserver);
          break;
        case METHODID_VSTREAM_RESULTS:
          serviceImpl.vStreamResults((binlogdata.Binlogdata.VStreamResultsRequest) request,
              (io.grpc.stub.StreamObserver<binlogdata.Binlogdata.VStreamResultsResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class QueryBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    QueryBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return queryservice.Queryservice.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Query");
    }
  }

  private static final class QueryFileDescriptorSupplier
      extends QueryBaseDescriptorSupplier {
    QueryFileDescriptorSupplier() {}
  }

  private static final class QueryMethodDescriptorSupplier
      extends QueryBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    QueryMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (QueryGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new QueryFileDescriptorSupplier())
              .addMethod(getExecuteMethod())
              .addMethod(getExecuteBatchMethod())
              .addMethod(getStreamExecuteMethod())
              .addMethod(getBeginMethod())
              .addMethod(getCommitMethod())
              .addMethod(getRollbackMethod())
              .addMethod(getPrepareMethod())
              .addMethod(getCommitPreparedMethod())
              .addMethod(getRollbackPreparedMethod())
              .addMethod(getCreateTransactionMethod())
              .addMethod(getStartCommitMethod())
              .addMethod(getSetRollbackMethod())
              .addMethod(getConcludeTransactionMethod())
              .addMethod(getReadTransactionMethod())
              .addMethod(getBeginExecuteMethod())
              .addMethod(getBeginExecuteBatchMethod())
              .addMethod(getMessageStreamMethod())
              .addMethod(getMessageAckMethod())
              .addMethod(getReserveExecuteMethod())
              .addMethod(getReserveBeginExecuteMethod())
              .addMethod(getReleaseMethod())
              .addMethod(getStreamHealthMethod())
              .addMethod(getVStreamMethod())
              .addMethod(getVStreamRowsMethod())
              .addMethod(getVStreamResultsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
