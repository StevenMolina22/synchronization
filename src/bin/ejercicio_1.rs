use std::sync::Arc;

use log::info;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{Duration, sleep};

const CAPACIDAD_CINTA: usize = 50;
const N_SOPLADORES: usize = 5;
const M_LLENADORES: usize = 5;
const K_EMPAQUETADORES: usize = 5;

const SOPLAR_MIN_MS: u64 = 100;
const SOPLAR_MAX_MS: u64 = 300;
const LLENAR_MIN_MS: u64 = 300;
const LLENAR_MAX_MS: u64 = 700;
const EMPAQUETAR_MIN_MS: u64 = 200;
const EMPAQUETAR_MAX_MS: u64 = 500;

#[derive(Debug, Clone, PartialEq)]
enum Slot {
    Libre,
    BotellaVacia,
    BotellaLlena,
}

struct Cinta {
    slots: Vec<Slot>,
    idx_para_soplar: usize,
    idx_para_llenar: usize,
    idx_para_empaquetar: usize,
}

impl Cinta {
    fn new(capacidad: usize) -> Self {
        Self {
            slots: vec![Slot::Libre; capacidad],
            idx_para_soplar: 0,
            idx_para_llenar: 0,
            idx_para_empaquetar: 0,
        }
    }
}

async fn proceso_soplador(
    id: usize,
    libre: Arc<Semaphore>,
    vacio: Arc<Semaphore>,
    cinta: Arc<Mutex<Cinta>>,
) {
    let mut rng = SmallRng::from_entropy();

    loop {
        let t = rng.gen_range(SOPLAR_MIN_MS..=SOPLAR_MAX_MS);
        info!("Soplador     {id:>2} | fabricando botella...  ({t} ms)");
        sleep(Duration::from_millis(t)).await;

        let libre_permit = libre.acquire().await.unwrap();

        {
            let mut c = cinta.lock().await;
            let idx = c.idx_para_soplar;
            c.slots[idx] = Slot::BotellaVacia;
            info!("Soplador     {id:>2} | deposito  VACIA    en slot {idx:>2}");
            c.idx_para_soplar = (idx + 1) % CAPACIDAD_CINTA;
        }

        libre_permit.forget();
        vacio.add_permits(1);
    }
}

async fn proceso_llenador(
    id: usize,
    vacio: Arc<Semaphore>,
    lleno: Arc<Semaphore>,
    cinta: Arc<Mutex<Cinta>>,
) {
    let mut rng = SmallRng::from_entropy();

    loop {
        let vacio_permit = vacio.acquire().await.unwrap();

        {
            let mut c = cinta.lock().await;
            let idx = c.idx_para_llenar;
            c.slots[idx] = Slot::BotellaLlena;
            info!("Llenador     {id:>2} | tomo      VACIA    en slot {idx:>2}");
            c.idx_para_llenar = (idx + 1) % CAPACIDAD_CINTA;
        }

        let t = rng.gen_range(LLENAR_MIN_MS..=LLENAR_MAX_MS);
        info!("Llenador     {id:>2} | llenando botella...    ({t} ms)");
        sleep(Duration::from_millis(t)).await;

        vacio_permit.forget();
        lleno.add_permits(1);
    }
}

async fn proceso_empaquetador(
    id: usize,
    lleno: Arc<Semaphore>,
    libre: Arc<Semaphore>,
    cinta: Arc<Mutex<Cinta>>,
) {
    let mut rng = SmallRng::from_entropy();

    loop {
        let lleno_permit = lleno.acquire().await.unwrap();

        {
            let mut c = cinta.lock().await;
            let idx = c.idx_para_empaquetar;
            c.slots[idx] = Slot::Libre;
            info!("Empaquetador {id:>2} | retiro    LLENA   en slot {idx:>2}");
            c.idx_para_empaquetar = (idx + 1) % CAPACIDAD_CINTA;
        }

        let t = rng.gen_range(EMPAQUETAR_MIN_MS..=EMPAQUETAR_MAX_MS);
        info!("Empaquetador {id:>2} | empaquetando...        ({t} ms)");
        sleep(Duration::from_millis(t)).await;

        lleno_permit.forget();
        libre.add_permits(1);
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_millis()
        .init();

    assert!(
        N_SOPLADORES <= CAPACIDAD_CINTA,
        "N_SOPLADORES supera la capacidad de la cinta"
    );
    assert!(
        M_LLENADORES <= CAPACIDAD_CINTA,
        "M_LLENADORES supera la capacidad de la cinta"
    );
    assert!(
        K_EMPAQUETADORES <= CAPACIDAD_CINTA,
        "K_EMPAQUETADORES supera la capacidad de la cinta"
    );

    let libre = Arc::new(Semaphore::new(CAPACIDAD_CINTA));
    let vacio = Arc::new(Semaphore::new(0));
    let lleno = Arc::new(Semaphore::new(0));
    let cinta = Arc::new(Mutex::new(Cinta::new(CAPACIDAD_CINTA)));

    let mut handles = Vec::new();

    for id in 0..N_SOPLADORES {
        handles.push(tokio::spawn(proceso_soplador(
            id,
            Arc::clone(&libre),
            Arc::clone(&vacio),
            Arc::clone(&cinta),
        )));
    }

    for id in 0..M_LLENADORES {
        handles.push(tokio::spawn(proceso_llenador(
            id,
            Arc::clone(&vacio),
            Arc::clone(&lleno),
            Arc::clone(&cinta),
        )));
    }

    for id in 0..K_EMPAQUETADORES {
        handles.push(tokio::spawn(proceso_empaquetador(
            id,
            Arc::clone(&lleno),
            Arc::clone(&libre),
            Arc::clone(&cinta),
        )));
    }

    for handle in handles {
        let _ = handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[test]
    fn cp1_test_inicializacion_y_aritmetica_circular() {
        let capacidad = 5;
        let mut cinta = Cinta::new(capacidad);

        assert_eq!(cinta.slots.len(), capacidad);
        assert_eq!(cinta.slots[0], Slot::Libre);
        assert_eq!(cinta.idx_para_soplar, 0);

        for _ in 0..capacidad {
            let idx = cinta.idx_para_soplar;
            cinta.idx_para_soplar = (idx + 1) % capacidad;
        }

        assert_eq!(cinta.idx_para_soplar, 0);
    }

    #[test]
    fn cp2_test_flujo_logico_botella() {
        let mut cinta = Cinta::new(50);

        let idx_soplar = cinta.idx_para_soplar;
        cinta.slots[idx_soplar] = Slot::BotellaVacia;
        cinta.idx_para_soplar = (idx_soplar + 1) % 50;
        assert_eq!(cinta.slots[0], Slot::BotellaVacia);

        let idx_llenar = cinta.idx_para_llenar;
        cinta.slots[idx_llenar] = Slot::BotellaLlena;
        cinta.idx_para_llenar = (idx_llenar + 1) % 50;
        assert_eq!(cinta.slots[0], Slot::BotellaLlena);

        let idx_empaquetar = cinta.idx_para_empaquetar;
        cinta.slots[idx_empaquetar] = Slot::Libre;
        cinta.idx_para_empaquetar = (idx_empaquetar + 1) % 50;
        assert_eq!(cinta.slots[0], Slot::Libre);
    }

    #[tokio::test]
    async fn cp3_test_ejecucion_concurrente_sin_deadlocks() {
        let libre = Arc::new(Semaphore::new(CAPACIDAD_CINTA));
        let vacio = Arc::new(Semaphore::new(0));
        let lleno = Arc::new(Semaphore::new(0));
        let cinta = Arc::new(Mutex::new(Cinta::new(CAPACIDAD_CINTA)));

        let _soplador = tokio::spawn(proceso_soplador(
            0,
            Arc::clone(&libre),
            Arc::clone(&vacio),
            Arc::clone(&cinta),
        ));
        let _llenador = tokio::spawn(proceso_llenador(
            0,
            Arc::clone(&vacio),
            Arc::clone(&lleno),
            Arc::clone(&cinta),
        ));
        let _empaquetador = tokio::spawn(proceso_empaquetador(
            0,
            Arc::clone(&lleno),
            Arc::clone(&libre),
            Arc::clone(&cinta),
        ));

        let cinta_obs = Arc::clone(&cinta);

        let result = timeout(Duration::from_secs(10), async move {
            let mut last_indices = (0, 0, 0);
            let mut tiempo_estancado = 0;

            loop {
                sleep(Duration::from_millis(200)).await;

                match cinta_obs.try_lock() {
                    Ok(c) => {
                        let indices_actuales =
                            (c.idx_para_soplar, c.idx_para_llenar, c.idx_para_empaquetar);

                        if indices_actuales == last_indices {
                            tiempo_estancado += 200;
                            assert!(
                                tiempo_estancado < 3000,
                                "Deadlock detectado: el sistema se congelo en {:?}",
                                indices_actuales
                            );
                        } else {
                            tiempo_estancado = 0;
                            last_indices = indices_actuales;
                        }
                    }
                    Err(_) => {
                        tiempo_estancado = 0;
                    }
                }
            }
        })
        .await;

        assert!(result.is_err(), "El test termino de forma inesperada");
    }
}
