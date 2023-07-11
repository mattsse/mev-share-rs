//! SSE server side support.

use futures_util::{Stream, StreamExt};
use http::{
    header::{CACHE_CONTROL, CONTENT_TYPE},
    StatusCode,
};
use hyper::{Body, Request, Response};
use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::ReaderStream};
use tower::{Layer, Service};

/// Layer that can create [`SseBroadcastService`].
#[derive(Clone)]
pub struct SseBroadcastLayer<F> {
    handler: F,
    path: Arc<String>,
}

impl<F> SseBroadcastLayer<F> {
    /// Creates a new [`SseBroadcastLayer`] with the given handler and the path to listen on.
    pub fn new(path: String, handler: F) -> Self {
        Self { path: Arc::new(path), handler }
    }
}

impl<F, S> Layer<S> for SseBroadcastLayer<F>
where
    F: Clone,
{
    type Service = SseBroadcastService<S, F>;

    fn layer(&self, inner: S) -> Self::Service {
        SseBroadcastService { inner, handler: self.handler.clone(), path: Arc::clone(&self.path) }
    }
}

/// A service that will stream messages into a http response.
///
/// Note: This will not set sse id's and will use the default "message" as event name.
pub struct SseBroadcastService<S, F> {
    path: Arc<String>,
    inner: S,
    handler: F,
}

impl<S, F, R, St> Service<Request<Body>> for SseBroadcastService<S, F>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Response: 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
    S::Future: Send + 'static,
    F: Fn() -> R,
    R: Future<Output = Result<St, Box<dyn Error + Send + Sync>>> + Send + 'static,
    St: Stream<Item = String> + Send + Unpin + 'static,
{
    type Response = S::Response;
    type Error = Box<dyn Error + Send + Sync>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // check if the request is for the path we are listening on for SSE
        if self.path.as_str() == request.uri() {
            // acquire the receiver and perform the upgrade
            let get_receiver = (self.handler)();
            let fut = async {
                let st = match get_receiver.await {
                    Ok(st) => st,
                    Err(err) => {
                        return Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from(err.to_string()))
                            .expect("failed to build response"))
                    }
                };
                let (sender, encoder) = async_sse::encode();

                tokio::task::spawn(async move {
                    let mut st = st;
                    while let Some(data) = st.next().await {
                        let _ = sender.send(None, data.as_str(), None).await;
                    }
                });

                // Perform the handshake as described here:
                // <https://html.spec.whatwg.org/multipage/server-sent-events.html#sse-processing-model>
                Response::builder()
                    .status(StatusCode::OK)
                    .header(CACHE_CONTROL, "no-cache")
                    .header(CONTENT_TYPE, "text/event-stream")
                    .body(Body::wrap_stream(ReaderStream::new(encoder.compat())))
                    .map_err(|err| err.into())
            };

            return Box::pin(fut)
        }

        // delegate to the inner service if path does not match
        let fut = self.inner.call(request);

        Box::pin(async move { fut.await.map_err(Into::into) })
    }
}
