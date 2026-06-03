# Synchronization

Ejemplos simples de sincronizacion entre procesos usando semaforos con Tokio.

## Ejercicios

- `ejercicio_1`: cinta transportadora con sopladores, llenadores y
  empaquetadores.
- `ejercicio_2`: sincronizacion por rondas entre procesos `RayTracer`.

## Ejecutar

```bash
cargo run --bin ejercicio_1
cargo run --bin ejercicio_2
```

El ejercicio 1 corre en loop infinito. Interrumpir con `Ctrl+C`.

Para ejecutar las pruebas:

```bash
cargo test
```

## Ejercicio 2: RayTracer

## Idea

El programa simula 4 procesos `RayTracer` que primero calculan iluminacion y
luego deben sincronizarse en 2 rondas antes de dibujar pixeles.

Cada proceso avisa a un companero y espera la senal de otro proceso antes de
avanzar.

## Sincronizacion

Se usa una matriz de semaforos:

```text
arrived[round][process]
```

Cada semaforo empieza en `0`.

- `add_permits(1)` representa `V`: avisar que el proceso llego.
- `acquire().await` representa `P`: esperar hasta recibir una senal.

## Rondas

Ronda 0:

```text
P0 <-> P1
P2 <-> P3
```

Ronda 1:

```text
P0 <-> P2
P1 <-> P3
```

Esto obliga a que todos los procesos pasen por puntos de sincronizacion antes
de continuar.

## Flujo

```text
calcular_iluminacion()
        |
   ronda 0 sync
        |
   ronda 1 sync
        |
 dibujar_pixeles()
```

La salida puede variar porque las tareas corren concurrentemente, pero todos los
procesos deben calcular iluminacion, pasar las dos rondas de sincronizacion y
dibujar pixeles.

## Objetivo

Mostrar como modelar sincronizacion entre tareas concurrentes usando semaforos
asincronos.
