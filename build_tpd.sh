cp ./target/thoughtspot-component-0.0.1-SNAPSHOT.car /opt/talend/downloads_exchange
docker run -v /opt/talend/pipeline-remote-engine/:/opt/talend/pipeline-remote-engine/ -v /opt/talend/downloads_exchange/:/opt/talend/downloads_exchange/ -v /var/run/docker.sock:/var/run/docker.sock tacokit/remote-engine-customizer:1.1.15_20191024084044 register-component-archive --remote-engine-dir=/opt/talend/pipeline-remote-engine/ --component-archive=/opt/talend/downloads_exchange/thoughtspot-component-0.0.1-SNAPSHOT.car

