FROM python:3.13.15-slim AS builder
WORKDIR /build
COPY pyproject.toml requirements-product.lock ./
COPY src ./src
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-product.lock \
    && python -m pip wheel --no-build-isolation --no-deps --wheel-dir /wheels .

FROM python:3.13.15-slim
RUN groupadd --system updatis && useradd --system --gid updatis --home-dir /nonexistent updatis
COPY --from=builder /wheels /wheels
COPY --from=builder /usr/local /usr/local
RUN python -m pip install --no-cache-dir --no-deps /wheels/updatis-*.whl && rm -rf /wheels
USER updatis
ENTRYPOINT ["python", "-m"]
CMD ["updatis.api"]
