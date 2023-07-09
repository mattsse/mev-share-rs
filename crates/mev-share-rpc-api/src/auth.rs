//! A layer responsible for implementing flashbots-style authentication
//! by signing the request body with a private key and adding the signature
//! to the request headers.

use std::{
    error::Error,
    task::{Context, Poll},
};

use ethers_core::{types::H256, utils::keccak256};
use ethers_signers::Signer;
use futures_util::future::BoxFuture;

use http::{header::HeaderValue, HeaderName, Request};
use hyper::Body;

use tower::{Layer, Service};

/// Layer that applies [`FlashbotsSigner`] which adds a request header with a signed payload.
#[derive(Clone)]
pub struct FlashbotsSignerLayer<S> {
    signer: S,
}

impl<S> FlashbotsSignerLayer<S> {
    /// Creates a new [`FlashbotsSignerLayer`] with the given signer.
    pub fn new(signer: S) -> Self {
        FlashbotsSignerLayer { signer }
    }
}

impl<S: Clone, I> Layer<I> for FlashbotsSignerLayer<S> {
    type Service = FlashbotsSigner<S, I>;

    fn layer(&self, inner: I) -> Self::Service {
        FlashbotsSigner { signer: self.signer.clone(), inner }
    }
}

/// Middleware that adds a request header with a signed payload.
/// For more info, see https://docs.flashbots.net/flashbots-auction/searchers/advanced/rpc-endpoint#authentication
#[derive(Clone)]
pub struct FlashbotsSigner<S, I> {
    signer: S,
    inner: I,
}

impl<S, I> Service<Request<Body>> for FlashbotsSigner<S, I>
where
    I: Service<Request<Body>> + Clone + Send + 'static,
    I::Future: Send,
    I::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
    S: Signer + Clone + Send + 'static,
{
    type Response = I::Response;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        // wait for service to be ready
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let signer = self.signer.clone();

        let (mut parts, body) = request.into_parts();

        // if method is not POST, or content-type is not json, just pass through the request
        if parts.method != http::Method::POST ||
            parts
                .headers
                .get(http::header::CONTENT_TYPE)
                .map(|v| v != HeaderValue::from_static("application/json"))
                .unwrap_or(true)
        {
            return Box::pin(async move {
                let request = Request::from_parts(parts, body);
                inner.call(request).await.map_err(Into::into)
            })
        }

        // otherwise, sign the request body and add the signature to the header
        Box::pin(async move {
            let body_bytes = hyper::body::to_bytes(body).await?;

            // sign request body and insert header
            let signature = signer
                .sign_message(format!("0x{:x}", H256::from(keccak256(body_bytes.clone()))))
                .await?;

            let header_name = HeaderName::from_static("x-flashbots-signature");
            let header_val =
                HeaderValue::from_str(&format!("{:?}:0x{}", signer.address(), signature)).unwrap();
            parts.headers.insert(header_name, header_val);

            let request = Request::from_parts(parts, Body::from(body_bytes.clone()));
            inner.call(request).await.map_err(Into::into)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers_core::rand::thread_rng;
    use ethers_signers::LocalWallet;
    use http::Response;
    use hyper::Body;
    use std::convert::Infallible;
    use tower::{service_fn, ServiceExt};

    #[tokio::test]
    async fn test_signature() {
        let fb_signer = LocalWallet::new(&mut thread_rng());

        // mock service that returns the request headers
        let svc = FlashbotsSigner {
            signer: fb_signer.clone(),
            inner: service_fn(|_req: Request<Body>| async {
                let (parts, _) = _req.into_parts();

                let mut res = Response::builder();
                for (k, v) in parts.headers.iter() {
                    res = res.header(k, v);
                }
                let res = res.body(Body::empty()).unwrap();
                Ok::<_, Infallible>(res)
            }),
        };

        // generate set of bytes for the payload
        let bytes = vec![1u8; 32];

        let res = svc.oneshot(Request::new(Body::from(bytes.clone()))).await.unwrap();

        let header = res.headers().get("x-flashbots-signature").unwrap();
        let header = header.to_str().unwrap();
        let header = header.split(":0x").collect::<Vec<_>>();
        let header_address = header[0];
        let header_signature = header[1];

        let signer_address = format!("{:?}", fb_signer.address());
        let expected_signature = fb_signer
            .sign_message(format!("0x{:x}", H256::from(keccak256(bytes.clone()))))
            .await
            .unwrap()
            .to_string();

        // verify that the header contains expected address and signature
        assert_eq!(header_address, signer_address);
        assert_eq!(header_signature, expected_signature);
    }
}