FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal

# curl is the healthcheck client. The package itself is the AWS Emulator LTS RPM.
# uname picks the RPM arch. The classic builder does not set TARGETARCH.
RUN microdnf install -y curl-minimal grep \
    && case "$(uname -m)" in \
         aarch64|arm64) arch=aarch64 ;; \
         x86_64|amd64) arch=x86_64 ;; \
         *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/aws-emulator.rpm \
         "https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/latest/aws-emulator-${arch}.rpm" \
    && rpm -i /tmp/aws-emulator.rpm \
    && rm /tmp/aws-emulator.rpm \
    && mkdir -p /home/emulator \
    && chown 1000 /home/emulator \
    && microdnf clean all

USER 1000
ENV HOME=/home/emulator \
    EMULATOR_BIND_ADDRESS=0.0.0.0
ENTRYPOINT ["aws-emulator"]
