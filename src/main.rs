use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};

const ROUNDS: usize = 2;
const PROCESSES: usize = 4;

type SemaphoreMatrix = Vec<Vec<Arc<Semaphore>>>;

#[tokio::main]
async fn main() {
    // Matriz de semaforos: 2 rondas x 4 procesos.
    // Cada semaforo empieza en 0 porque todavia no llego ninguna senal.
    // Arc permite que todos los RayTracer compartan los mismos semaforos.
    let arrived: SemaphoreMatrix = (0..ROUNDS)
        .map(|_| {
            (0..PROCESSES)
                .map(|_| Arc::new(Semaphore::new(0)))
                .collect()
        })
        .collect();

    let partners = [[1, 0, 3, 2], [2, 3, 0, 1]];
    let mut tasks = Vec::new();

    for my_id in 0..PROCESSES {
        let arrived = arrived.clone();

        tasks.push(tokio::spawn(async move {
            ray_tracer(my_id, arrived, partners).await;
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

async fn ray_tracer(
    my_id: usize,
    arrived: SemaphoreMatrix,
    partners: [[usize; PROCESSES]; ROUNDS],
) {
    calcular_iluminacion(my_id).await;

    for round in 0..ROUNDS {
        let partner = partners[round][my_id];

        println!("RayTracer {my_id}: avisa a P{partner} en ronda {round}");
        // V(arrived[round][partner]): aviso a mi companero que llegue.
        arrived[round][partner].add_permits(1);

        println!("RayTracer {my_id}: espera su senal en ronda {round}");
        // P(arrived[round][my_id]): espero la senal de mi companero.
        let permit = arrived[round][my_id].acquire().await.unwrap();
        drop(permit);

        println!("RayTracer {my_id}: paso la ronda {round}");
    }

    dibujar_pixeles(my_id).await;
}

async fn calcular_iluminacion(my_id: usize) {
    println!("RayTracer {my_id}: calculando iluminacion");

    let delay = match my_id {
        0 => 500,
        1 => 1000,
        2 => 1500,
        3 => 2000,
        _ => 500,
    };

    sleep(Duration::from_millis(delay)).await;

    println!("RayTracer {my_id}: termino fase 1");
}

async fn dibujar_pixeles(my_id: usize) {
    println!("RayTracer {my_id}: dibujando pixeles");
}
