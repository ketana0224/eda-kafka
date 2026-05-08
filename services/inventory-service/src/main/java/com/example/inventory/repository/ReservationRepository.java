package com.example.inventory.repository;

import com.example.inventory.model.Reservation;
import com.example.inventory.model.ReservationStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {

    boolean existsByOrderIdAndStatus(String orderId, ReservationStatus status);

    List<Reservation> findByOrderIdAndStatus(String orderId, ReservationStatus status);
}
