# Fred plugin Spider

The plugin Library for processing data received from Spider gives a large cpu load.
This may cause the wrapper to restart jvm.
To avoid this scenario, consider running Fred without a wrapper:
`java -Xmx8g -cp freenet.jar:freenet-ext.jar:bcprov-jdk15on-1.59.jar:jna-4.2.2.jar:jna-platform-4.2.2.jar freenet.node.NodeStarter`
