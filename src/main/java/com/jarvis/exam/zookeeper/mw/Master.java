package com.jarvis.exam.zookeeper.mw;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.KeeperException.*;

@Slf4j
public class Master {

    private ZooKeeper zk;

    private String connection;

    private String serverId = Integer.toHexString(new Random().nextInt());
    private boolean leader = false;

    public Master(String connection) {
        this.connection = connection;
    }

    public void runForMaster() throws InterruptedException, KeeperException {
        while (true) {
            try {
                // 마스터가 되기 위해 임시 노드를 생성한다.
                zk.create("/master",
                        serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);

                leader = true;
                break;
            } catch (NodeExistsException e) {
                // 이미 마스터 노드가 존재하면 다른 프로세스가 마스터가 되어 있다.
                leader = false;
                break;
            } catch (ConnectionLossException e) {
                log.warn("connection loss with zookeeper", e);
                // ConnectionLossException은 예외 처리를 하는 이유는 Zookeeper 서버와 연결이 성공 했을 수도 있기 때문이다.
                // 위 예외가 발생 하는 경우는 클라이언트와 주키퍼 서버가 연결이 끊어졌을 때 발생하는데,
                // 네트워크 단절과 같은 네트워크 에러나 주키퍼 서버의 장애 상황에 발생한다.
                // 클라이언트는 예외가 발생했을때 요청한 작업이 완료 하기전에 끊긴것인지 완료는 했지만 클라이언트가 응답을 받지 못한것인지 알 수가 없다.
                //
                // 주키퍼 클라이언트 라이브러리는 다음 요청을 위해 서버와 재연결되지만,
                // 프로레스는 보류 중인 요청이 잘 처리된것인지, 다시 재요청을 해야하는지 판단해야한다.
            }

            if (checkMaster()) {
                break;
            }
        }
    }

    /**
     * 누가 리더인지 확인한다.
     * ConnectionLossException을 수신한 경우에도 자신이 마스터 일 수도 있기 때문이다.
     * 이 함수가 호출 되는 경우는 create 요청은 Zookeeper에서 처리 되어있지만 클라이언트가 수신을 받지 못한경우이다.
     *
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    private boolean checkMaster() throws InterruptedException, KeeperException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                leader = new String(data).equals(serverId);
                return true;
            } catch (NoNodeException e) {
                e.printStackTrace();
                return false;
            } catch (ConnectionLossException e) {
                log.warn("connection loss with zookeeper", e);

            }
        }
    }


    public void startZk() throws IOException {
        zk = new ZooKeeper(connection, 15000, System.out::println);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public boolean isLeader() {
        return leader;
    }
}
