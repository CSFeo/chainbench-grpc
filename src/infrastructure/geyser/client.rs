use {
    bytes::Bytes,
    futures::{channel::mpsc, sink::Sink, stream::Stream},
    std::convert::TryInto,
    tonic::{
        Request, Response, Status,
        codec::Streaming,
        metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
        service::interceptor::InterceptedService,
        transport::{ClientTlsConfig, Endpoint, channel::Channel},
    },
};

use crate::infrastructure::proto::geyser::{
    SubscribeRequest, SubscribeUpdate, geyser_client::GeyserClient,
};

#[derive(Clone, Debug)]
pub struct InterceptorXToken {
    pub x_token: Option<AsciiMetadataValue>,
}

impl tonic::service::Interceptor for InterceptorXToken {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", token);
        }
        Ok(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcClientError {
    #[error("gRPC status: {0}")]
    TonicStatus(#[from] Status),
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

pub struct GeyserGrpcClient {
    geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
}

impl GeyserGrpcClient {
    pub fn build_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        Ok(GeyserGrpcBuilder::new(Endpoint::from_shared(endpoint)?))
    }

    /// Open a subscription. The caller drives the returned sink to send the
    /// `SubscribeRequest` (filters, commitment) and reads updates from the stream.
    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        let (subscribe_tx, subscribe_rx) = mpsc::unbounded();
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    fn new(geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>) -> Self {
        Self { geyser }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcBuilderError {
    #[error("Failed to parse x-token: {0}")]
    MetadataValueError(#[from] InvalidMetadataValue),
    #[error("gRPC transport error: {0}")]
    TonicError(#[from] tonic::transport::Error),
}

pub type GeyserGrpcBuilderResult<T> = Result<T, GeyserGrpcBuilderError>;

pub struct GeyserGrpcBuilder {
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
}

impl GeyserGrpcBuilder {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            x_token: None,
        }
    }

    pub async fn connect(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect().await?;
        self.build(channel)
    }

    fn build(self, channel: Channel) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let interceptor = InterceptorXToken {
            x_token: self.x_token,
        };
        let geyser = GeyserClient::with_interceptor(channel, interceptor);
        Ok(GeyserGrpcClient::new(geyser))
    }

    pub fn x_token<T>(mut self, x_token: Option<T>) -> GeyserGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        self.x_token = x_token.map(|value| value.try_into()).transpose()?;
        Ok(self)
    }

    pub fn tls_config(mut self, tls_config: ClientTlsConfig) -> GeyserGrpcBuilderResult<Self> {
        self.endpoint = self.endpoint.tls_config(tls_config)?;
        Ok(self)
    }
}
